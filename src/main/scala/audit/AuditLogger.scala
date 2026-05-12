package audit

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import java.time.Instant

class AuditLogger(filePath: String) {
  private val path = Paths.get(filePath)
  private val parent = path.getParent
  if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
  if (!Files.exists(path)) Files.createFile(path)

  private def appendJson(line: String): Unit = {
    val bytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)
    Files.write(path, bytes, StandardOpenOption.APPEND)
  }

  def recordStageMetrics(
    runId: String,
    stage: String,
    rowsIn: Long,
    rowsOut: Long,
    durationMs: Long,
    status: String = "SUCCESS",
    message: Option[String] = None
  ): Unit = {
    val msg = message.map { m =>
      val safe = m.replace("\"", "'")
      s",\"message\":\"$safe\""
    }.getOrElse("")
    val json = s"{\"ts\":\"${Instant.now().toString}\",\"type\":\"stage_metrics\",\"runId\":\"$runId\",\"stage\":\"$stage\",\"rowsIn\":$rowsIn,\"rowsOut\":$rowsOut,\"durationMs\":$durationMs,\"status\":\"$status\"$msg}"
    appendJson(json)
  }

  def recordRunSummary(
    runId: String,
    startTime: String,
    endTime: String,
    status: String,
    totalExtracted: Long,
    totalLoaded: Long
  ): Unit = {
    val json = s"{\"ts\":\"${Instant.now().toString}\",\"type\":\"run_summary\",\"runId\":\"$runId\",\"startTime\":\"$startTime\",\"endTime\":\"$endTime\",\"status\":\"$status\",\"totalExtracted\":$totalExtracted,\"totalLoaded\":$totalLoaded}"
    appendJson(json)
  }
}
