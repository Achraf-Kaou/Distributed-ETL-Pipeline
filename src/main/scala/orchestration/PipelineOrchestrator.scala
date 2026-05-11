package orchestration

import logging.PipelineLogger

class PipelineOrchestrator(
  val stages: Seq[String],
  failFast: Boolean,
  logger: PipelineLogger
) {
  private val activeStages =
    if (stages.nonEmpty) stages.map(_.trim.toLowerCase).filter(_.nonEmpty)
    else Seq("extract", "clean", "dedup", "join", "aggregate", "build_star", "load")

  def shouldRun(stage: String): Boolean = activeStages.contains(stage.trim.toLowerCase)

  def runStage[T](stage: String)(action: => T): Option[T] = {
    if (!shouldRun(stage)) return None

    logger.stepStartLog(stage)
    try {
      val result = action
      logger.stepEndLog(stage)
      Some(result)
    } catch {
      case e: Exception =>
        logger.error(s"Stage '$stage' failed: ${e.getMessage}")
        if (failFast) throw e
        None
    }
  }
}
