package orchestration

import logging.PipelineLogger
import audit.AuditLogger
import org.apache.spark.sql.DataFrame

/**
 * PipelineOrchestrator — stage lifecycle manager for the ETL pipeline.
 *
 * Responsibilities:
 *   1. Decide whether a given stage should execute at all (based on the
 *      `stages` list from `application.conf`).
 *   2. Wrap every stage invocation with start/end logging and wall-clock timing.
 *   3. Write a per-stage audit record (rows in, rows out, duration, status).
 *   4. Handle stage failures: either propagate immediately (`failFast = true`)
 *      or swallow the exception and return `None` so the pipeline continues.
 *
 * Stage filtering lets operators run a subset of the pipeline during development
 * or re-processing. For example, setting `stages = ["load"]` in config will skip
 * all upstream stages and go straight to the load step.
 *
 * All stage actions are passed as by-name parameters (`=> T`), which means Spark
 * DAGs inside those blocks are only built and submitted when the stage is actually
 * scheduled to run — consistent with Spark's lazy evaluation model.
 *
 * @param stages       Ordered list of stage names to execute, from `application.conf`.
 *                     If empty, all known stages are enabled by default.
 * @param failFast     If true, re-throw exceptions immediately; the pipeline stops.
 *                     If false, log the error and return `None` for that stage so
 *                     subsequent stages can still attempt to run.
 * @param logger       Shared [[PipelineLogger]] for structured log output.
 * @param auditLogger  Shared [[AuditLogger]] for per-stage JSON audit records.
 * @param runId        UUID string identifying the current pipeline invocation,
 *                     propagated into every audit record for correlation.
 */
class PipelineOrchestrator(
  val stages:   Seq[String],
  failFast:     Boolean,
  logger:       PipelineLogger,
  auditLogger:  AuditLogger,
  runId:        String
) {

  /**
   * The resolved, normalised set of stages that should run.
   *
   * If the caller passes an empty list (no `stages` key in config), we fall back
   * to the full ordered default sequence so the pipeline runs end-to-end without
   * any explicit configuration.
   */
  private val activeStages: Seq[String] =
    if (stages.nonEmpty) stages.map(_.trim.toLowerCase).filter(_.nonEmpty)
    else Seq("extract", "clean", "dedup", "join", "aggregate", "build_star", "load")

  /**
   * Returns true if the given stage is in the active stages set.
   * Stage names are compared case-insensitively and with leading/trailing whitespace stripped.
   *
   * @param stage  Stage name to check, e.g. `"clean"`.
   */
  def shouldRun(stage: String): Boolean = activeStages.contains(stage.trim.toLowerCase)

  /**
   * Execute a pipeline stage with full lifecycle management.
   *
   * The method:
   *   1. Returns `None` immediately if the stage is not in `activeStages`.
   *   2. Calls [[PipelineLogger.stepStartLog]] and records start time.
   *   3. Evaluates the `action` block (by-name — built lazily).
   *   4. If the result is a [[DataFrame]], calls `.count()` to materialise the
   *      Spark DAG and obtain the output row count for the audit record.
   *      Note: this triggers an Action on the Spark cluster.
   *   5. Calls [[PipelineLogger.stepEndLog]] and writes a SUCCESS audit record.
   *   6. On exception: writes a FAILED audit record, then either re-throws
   *      (failFast=true) or returns `None` (failFast=false).
   *
   * Type parameter `T` is generic so the same method handles both DataFrame-
   * producing stages (extract, clean, dedup, join) and Unit-returning stages (load).
   *
   * @param stage   Stage name, must match an entry in `activeStages` to execute.
   * @param rowsIn  Input row count for the audit record. Pass the previous stage's
   *                output count. Defaults to 0 for stages where it isn't tracked.
   * @param action  The stage computation, evaluated lazily.
   * @tparam T      Return type of the stage — typically `DataFrame` or `Unit`.
   * @return        `Some(result)` on success, `None` if skipped or failed (failFast=false).
   */
  def runStage[T](
    stage:  String,
    rowsIn: => Long = 0L
  )(action: => T): Option[T] = {

    // Skip stages that are not listed in the active set.
    if (!shouldRun(stage)) return None

    logger.stepStartLog(stage)
    val startTime = System.currentTimeMillis()

    try {
      val result    = action
      val duration  = System.currentTimeMillis() - startTime

      // Materialise row count only for DataFrame results; Unit stages get 0.
      val rowsOut = result match {
        case df: DataFrame => df.count()
        case _             => 0L
      }

      logger.stepEndLog(stage, Some(rowsOut))
      auditLogger.recordStageMetrics(
        runId      = runId,
        stage      = stage,
        rowsIn     = rowsIn,
        rowsOut    = rowsOut,
        durationMs = duration
      )
      Some(result)

    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"Stage '$stage' failed: ${e.getMessage}")
        auditLogger.recordStageMetrics(
          runId      = runId,
          stage      = stage,
          rowsIn     = rowsIn,
          rowsOut    = 0L,
          durationMs = duration,
          status     = "FAILED",
          message    = Some(e.getMessage)
        )
        if (failFast) throw e
        None
    }
  }
}
