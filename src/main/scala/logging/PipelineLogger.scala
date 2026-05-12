package logging

import org.slf4j.LoggerFactory
import scala.collection.mutable

/**
 * PipelineLogger — thin SLF4J wrapper with built-in per-stage timing.
 *
 * Centralises all logging for the ETL pipeline so that:
 *   - Every log line goes through SLF4J → Log4j 2, and is therefore captured
 *     both on the console (human-readable pattern) and in the rolling JSON log
 *     file configured in `log4j2.xml`.
 *   - Stage start/end events carry wall-clock duration automatically, making
 *     it easy to spot slow stages without adding timing boilerplate everywhere.
 *
 * Usage pattern inside a pipeline stage:
 * {{{
 *   logger.stepStartLog("clean")
 *   val result = doClean(df)
 *   logger.stepEndLog("clean", Some(result.count()))
 *   // → logs: "Step 'clean' completed, rows=4850, durationMs=1240"
 * }}}
 *
 * The `metricsEnabled` flag controls whether timing data is collected.
 * When disabled, `durationMs=0` is emitted and no timing map is maintained —
 * useful for unit tests where wall-clock timing adds noise.
 *
 * @param metricsEnabled  If true, record step start times and emit duration
 *                        in `stepEndLog`. Defaults to true.
 */
class PipelineLogger(metricsEnabled: Boolean = true) {

  /** Underlying SLF4J logger; routed to Log4j 2 via the log4j-slf4j2-impl bridge. */
  private val logger = LoggerFactory.getLogger("ETL.Pipeline")

  /**
   * Mutable map of `step name → start timestamp (ms)`.
   * Populated by [[stepStartLog]] and consumed (not removed) by [[stepEndLog]].
   * Only used when `metricsEnabled = true`.
   */
  private val stepStart = mutable.Map.empty[String, Long]

  private def now: Long = System.currentTimeMillis()

  // ─────────────────────────────────────────────────────────────────
  // Standard leveled logging delegates
  // ─────────────────────────────────────────────────────────────────

  /** Log an informational message (stage progress, row counts, etc.). */
  def info(message: String): Unit = logger.info(message)

  /** Log a warning — non-fatal issues such as a skipped source or quality violation. */
  def warn(message: String): Unit = logger.warn(message)

  /**
   * Log an error, optionally with the causing exception for stack-trace capture.
   *
   * @param message    Human-readable error description.
   * @param throwable  The underlying exception, or `null` to omit stack trace.
   */
  def error(message: String, throwable: Throwable = null): Unit = {
    if (throwable != null) logger.error(message, throwable)
    else logger.error(message)
  }

  // ─────────────────────────────────────────────────────────────────
  // Stage timing helpers — called by PipelineOrchestrator
  // ─────────────────────────────────────────────────────────────────

  /**
   * Mark the start of a named pipeline stage and emit an INFO log line.
   * If `metricsEnabled`, stores the current timestamp for later duration
   * calculation in [[stepEndLog]].
   *
   * @param step  Stage name as declared in `orchestration.stages`, e.g. `"clean"`.
   */
  def stepStartLog(step: String): Unit = {
    if (metricsEnabled) stepStart.update(step, now)
    info(s"Step '$step' started")
  }

  /**
   * Mark the end of a named pipeline stage and emit an INFO log line that
   * includes optional row count and elapsed duration.
   *
   * Duration is computed as `now - startTime` captured in [[stepStartLog]].
   * If the step name was never started (e.g. metricsEnabled=false), duration is 0.
   *
   * @param step  Stage name matching the earlier [[stepStartLog]] call.
   * @param rows  Optional output row count to include in the log message.
   */
  def stepEndLog(step: String, rows: Option[Long] = None): Unit = {
    val elapsed = if (metricsEnabled) stepStart.get(step).map(s => now - s).getOrElse(0L) else 0L
    val rowsMsg = rows.map(r => s", rows=$r").getOrElse("")
    val timeMsg = if (metricsEnabled) s", durationMs=$elapsed" else ""
    info(s"Step '$step' completed$rowsMsg$timeMsg")
  }
}