package audit

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * AuditLogger — append-only, newline-delimited JSON audit trail for a pipeline run.
 *
 * Every pipeline execution writes two kinds of records to a single flat file:
 *   - `stage_metrics` — emitted after each ETL stage completes (or fails), capturing
 *     row counts in/out and wall-clock duration in milliseconds.
 *   - `run_summary`   — emitted once at the very end of a run, capturing overall
 *     status (SUCCESS / FAILED), timestamps, and total record counts.
 *
 * The file format is one JSON object per line (JSON Lines / NDJSON), which makes it
 * easy to tail with `cat`, query with `jq`, or ingest into a monitoring system later.
 *
 * The audit file is created (including parent directories) on construction so that
 * the first `append` call never races against directory creation.
 *
 * @param filePath  Absolute or relative path to the audit log file.
 *                  Example: `"output/final/warehouse/audit.log"`
 */
class AuditLogger(filePath: String) {

  private val path   = Paths.get(filePath)
  private val parent = path.getParent

  // Eagerly create the directory tree and the file itself so that subsequent
  // appends never fail due to a missing parent directory.
  if (parent != null && !Files.exists(parent)) Files.createDirectories(parent)
  if (!Files.exists(path)) Files.createFile(path)

  // ─────────────────────────────────────────────────────────────────
  // Internal helpers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Append a single JSON line to the audit file.
   * Uses `StandardOpenOption.APPEND` to guarantee that concurrent writes
   * from separate pipeline processes do not truncate existing content.
   *
   * @param line  A complete JSON object string (no trailing newline needed).
   */
  private def appendJson(line: String): Unit = {
    val bytes = (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8)
    Files.write(path, bytes, StandardOpenOption.APPEND)
  }

  // ─────────────────────────────────────────────────────────────────
  // Public API
  // ─────────────────────────────────────────────────────────────────

  /**
   * Record metrics for a single pipeline stage.
   *
   * Called by [[orchestration.PipelineOrchestrator]] immediately after each
   * stage completes (or fails), so every stage has a corresponding audit entry
   * regardless of outcome.
   *
   * Example JSON record:
   * {{{
   * {
   *   "ts":          "2024-06-01T10:15:30.123Z",
   *   "type":        "stage_metrics",
   *   "runId":       "a3f2...",
   *   "stage":       "clean",
   *   "rowsIn":      5000,
   *   "rowsOut":     4850,
   *   "durationMs":  1240,
   *   "status":      "SUCCESS"
   * }
   * }}}
   *
   * @param runId       UUID identifying the current pipeline run.
   * @param stage       Stage name, e.g. `"extract"`, `"clean"`, `"dedup"`.
   * @param rowsIn      Row count entering this stage (0 for non-DataFrame stages).
   * @param rowsOut     Row count produced by this stage.
   * @param durationMs  Wall-clock duration in milliseconds.
   * @param status      `"SUCCESS"` or `"FAILED"`.
   * @param message     Optional error message on failure (quotes escaped to single quotes).
   */
  def recordStageMetrics(
    runId:      String,
    stage:      String,
    rowsIn:     Long,
    rowsOut:    Long,
    durationMs: Long,
    status:     String        = "SUCCESS",
    message:    Option[String] = None
  ): Unit = {
    val msg = message.map { m =>
      val safe = m.replace("\"", "'")
      s""","message":"$safe""""
    }.getOrElse("")

    val json =
      s"""{"ts":"${Instant.now().toString}","type":"stage_metrics","runId":"$runId","stage":"$stage","rowsIn":$rowsIn,"rowsOut":$rowsOut,"durationMs":$durationMs,"status":"$status"$msg}"""
    appendJson(json)
  }

  /**
   * Record an end-of-run summary entry.
   *
   * Called once by [[Main.EtlPipeline]] in both the success and failure paths,
   * providing a single top-level record that represents the full pipeline run.
   *
   * Example JSON record:
   * {{{
   * {
   *   "ts":             "2024-06-01T10:15:55.400Z",
   *   "type":           "run_summary",
   *   "runId":          "a3f2...",
   *   "startTime":      "2024-06-01T10:15:00.000Z",
   *   "endTime":        "2024-06-01T10:15:55.400Z",
   *   "status":         "SUCCESS",
   *   "totalExtracted": 5000,
   *   "totalLoaded":    0
   * }
   * }}}
   *
   * @param runId          UUID identifying this pipeline run.
   * @param startTime      ISO-8601 timestamp of when the run started.
   * @param endTime        ISO-8601 timestamp of when the run ended.
   * @param status         `"SUCCESS"` or `"FAILED"`.
   * @param totalExtracted Total rows extracted across all sources.
   * @param totalLoaded    Total rows written to the warehouse target (0 if not tracked).
   */
  def recordRunSummary(
    runId:          String,
    startTime:      String,
    endTime:        String,
    status:         String,
    totalExtracted: Long,
    totalLoaded:    Long
  ): Unit = {
    val json =
      s"""{"ts":"${Instant.now().toString}","type":"run_summary","runId":"$runId","startTime":"$startTime","endTime":"$endTime","status":"$status","totalExtracted":$totalExtracted,"totalLoaded":$totalLoaded}"""
    appendJson(json)
  }
}
