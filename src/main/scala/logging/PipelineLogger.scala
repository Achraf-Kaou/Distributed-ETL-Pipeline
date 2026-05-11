package logging

import scala.collection.mutable

class PipelineLogger(level: String = "INFO", metricsEnabled: Boolean = true) {
  private val stepStart = mutable.Map.empty[String, Long]

  private def now: Long = System.currentTimeMillis()
  private def allowed(target: String): Boolean = {
    val order = Map("ERROR" -> 1, "WARN" -> 2, "INFO" -> 3, "DEBUG" -> 4)
    order.getOrElse(level.toUpperCase, 3) >= order.getOrElse(target.toUpperCase, 3)
  }

  def info(message: String): Unit = if (allowed("INFO")) println(s"[INFO] $message")
  def warn(message: String): Unit = if (allowed("WARN")) println(s"[WARN] $message")
  def error(message: String): Unit = if (allowed("ERROR")) println(s"[ERROR] $message")

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
