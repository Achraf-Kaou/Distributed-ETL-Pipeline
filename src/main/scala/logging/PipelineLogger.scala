package logging

import org.slf4j.LoggerFactory
import scala.collection.mutable

class PipelineLogger(metricsEnabled: Boolean = true) {
  private val logger = LoggerFactory.getLogger("ETL.Pipeline")
  private val stepStart = mutable.Map.empty[String, Long]

  private def now: Long = System.currentTimeMillis()

  def info(message: String): Unit = logger.info(message)
  def warn(message: String): Unit = logger.warn(message)
  def error(message: String, throwable: Throwable = null): Unit = {
    if (throwable != null) logger.error(message, throwable)
    else logger.error(message)
  }

  def stepStartLog(step: String): Unit = {
    if (metricsEnabled) stepStart.update(step, now)
    info(s"Step '$step' started")
  }

  def stepEndLog(step: String, rows: Option[Long] = None): Unit = {
    val elapsed = if (metricsEnabled) stepStart.get(step).map(s => now - s).getOrElse(0L) else 0L
    val rowsMsg = rows.map(r => s", rows=$r").getOrElse("")
    val timeMsg = if (metricsEnabled) s", durationMs=$elapsed" else ""
    info(s"Step '$step' completed$rowsMsg$timeMsg")
  }
}