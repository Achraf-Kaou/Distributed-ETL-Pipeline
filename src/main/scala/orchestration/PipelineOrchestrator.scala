package orchestration

import logging.PipelineLogger
import audit.AuditLogger
import org.apache.spark.sql.DataFrame

class PipelineOrchestrator(
  val stages: Seq[String],
  failFast: Boolean,
  logger: PipelineLogger,
  auditLogger: AuditLogger,
  runId: String
) {
  private val activeStages =
    if (stages.nonEmpty) stages.map(_.trim.toLowerCase).filter(_.nonEmpty)
    else Seq("extract", "clean", "dedup", "join", "aggregate", "build_star", "load")

  def shouldRun(stage: String): Boolean = activeStages.contains(stage.trim.toLowerCase)

  def runStage[T](
    stage: String,
    rowsIn: => Long = 0L
  )(action: => T): Option[T] = {

    if (!shouldRun(stage)) return None

    logger.stepStartLog(stage)
    val startTime = System.currentTimeMillis()

    try {
      val result = action
      val duration = System.currentTimeMillis() - startTime
      val rowsOut = result match {
        case df: DataFrame => df.count()
        case _ => 0L
      }

      logger.stepEndLog(stage, Some(rowsOut))
      auditLogger.recordStageMetrics(
        runId = runId,
        stage = stage,
        rowsIn = rowsIn,
        rowsOut = rowsOut,
        durationMs = duration
      )
      Some(result)
    } catch {
      case e: Exception =>
        val duration = System.currentTimeMillis() - startTime
        logger.error(s"Stage '$stage' failed: ${e.getMessage}")
        auditLogger.recordStageMetrics(
          runId = runId,
          stage = stage,
          rowsIn = rowsIn,
          rowsOut = 0L,
          durationMs = duration,
          status = "FAILED",
          message = Some(e.getMessage)
        )
        if (failFast) throw e
        None
    }
  }
}
