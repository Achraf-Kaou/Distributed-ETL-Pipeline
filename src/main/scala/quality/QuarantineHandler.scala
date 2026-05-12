package quality

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, not}
import logging.PipelineLogger
import java.time.Instant

/**
 * QuarantineHandler — bad-row isolation for the ETL pipeline.
 *
 * Rather than silently dropping invalid rows or letting them poison downstream
 * aggregations and joins, this handler physically separates them into a dedicated
 * Parquet "quarantine zone". This pattern is a standard practice in production
 * data pipelines and provides:
 *
 *   - **Auditability**: every rejected row is preserved with its rejection reason
 *     and timestamp so data teams can investigate root causes.
 *   - **Recovery**: quarantined rows can be re-processed after upstream fixes
 *     without re-running the entire pipeline.
 *   - **Clean downstream data**: stages after quarantine work on a validated
 *     subset, reducing the risk of cascading data quality issues.
 *
 * Output path structure:
 * {{{
 *   output/quarantine/<runId>/<stage>_<reason>/
 *     part-00000-....parquet   ← bad rows + quarantine_reason + quarantine_timestamp
 * }}}
 *
 * Each call to [[quarantineAndClean]] performs two Spark Actions:
 *   1. `.count()` — to check whether there are any bad rows before writing.
 *   2. `.write`   — materialise and persist the bad rows.
 *
 * The clean rows are returned as a filtered DataFrame (a lazy transformation —
 * no additional Action is triggered by this method itself).
 *
 * @param spark   Active [[SparkSession]] — required for DataFrame operations.
 * @param logger  [[PipelineLogger]] for warning messages when rows are quarantined.
 * @param quarantineBasePath Base directory for all quarantined data within this pipeline instance.
 */
class QuarantineHandler(
  spark: SparkSession, 
  logger: PipelineLogger,
  quarantineBasePath: String = "output/quarantine") {

  /** Base directory for all quarantined data within this pipeline instance. */
  private val quarantineBasePath = "output/quarantine"

  /**
   * Separate rows that violate critical-column constraints from the clean set.
   *
   * A row is considered "bad" if any column in `criticalColumns` is null.
   * Critical columns are business keys (id, email, employee_id) that make a row
   * meaningless without them — they cannot be filled with defaults because there
   * is no sensible value to substitute.
   *
   * The method writes bad rows to a Parquet partition under [[quarantineBasePath]]
   * and returns only the clean rows for further processing.
   *
   * If `criticalColumns` is empty, a fallback condition (`col("dummy").isNull`)
   * is used, which effectively matches nothing — the full DataFrame is returned
   * unchanged and nothing is quarantined.
   *
   * @param df              Input DataFrame to evaluate.
   * @param runId           Pipeline run UUID (used to namespace the output path).
   * @param stage           Current stage name, e.g. `"clean"`, `"dedup"` (used in path).
   * @param criticalColumns Column names that must be non-null. Rows violating ANY
   *                        of these constraints are quarantined.
   * @param reason          Short label written into the `quarantine_reason` column
   *                        and the output path. Defaults to `"critical_nulls"`.
   * @return                DataFrame containing only rows that pass all constraints.
   */
  def quarantineAndClean(
    df:              DataFrame,
    runId:           String,
    stage:           String,
    criticalColumns: Seq[String],
    reason:          String = "critical_nulls"
  ): DataFrame = {

    // Short-circuit: if the DataFrame is already empty, skip all Actions.
    if (df.isEmpty) return df

    // Build a compound OR condition: row is bad if ANY critical column is null.
    val badRowsCondition = criticalColumns
      .map(columnName => col(columnName).isNull)
      .reduceOption(_ || _)
      .getOrElse(col("dummy").isNull) // Fallback: never matches — no-op quarantine.

    val badRows  = df.filter(badRowsCondition)
    val badCount = badRows.count()

    if (badCount > 0) {
      val quarantinePath = s"$quarantineBasePath/$runId/${stage}_$reason"

      // Enrich bad rows with metadata before persisting.
      badRows
        .withColumn("quarantine_reason",    lit(reason))
        .withColumn("quarantine_timestamp", lit(Instant.now().toString))
        .write
        .mode(SaveMode.Append)  // Append: multiple stages can write to the same runId dir.
        .parquet(quarantinePath)

      logger.warn(s"QUARANTINE | $badCount rows moved to $quarantinePath (reason: $reason)")
    }

    // Return the complement — rows that pass all constraints.
    // This is a lazy filter; no additional Spark Action is triggered here.
    df.filter(not(badRowsCondition))
  }

  /**
   * Quarantine rows matching an arbitrary Spark SQL Column expression.
   *
   * Use this variant when the rejection criterion is more complex than a null check,
   * for example:
   * {{{
   *   handler.quarantineCustom(df, runId, "clean", col("age") < 0, "invalid_age")
   * }}}
   *
   * @param df               Input DataFrame.
   * @param runId            Pipeline run UUID.
   * @param stage            Current stage name.
   * @param filterCondition  Spark Column expression that evaluates to true for bad rows.
   * @param reason           Label for the quarantine output directory and metadata column.
   * @return                 DataFrame with matching rows removed.
   */
  def quarantineCustom(
    df:              DataFrame,
    runId:           String,
    stage:           String,
    filterCondition: org.apache.spark.sql.Column,
    reason:          String
  ): DataFrame = {

    val badRows  = df.filter(filterCondition)
    val badCount = badRows.count()

    if (badCount > 0) {
      val path = s"$quarantineBasePath/$runId/${stage}_$reason"
      badRows
        .withColumn("quarantine_reason",    lit(reason))
        .withColumn("quarantine_timestamp", lit(Instant.now().toString))
        .write.mode(SaveMode.Append).parquet(path)

      logger.warn(s"QUARANTINE | $badCount bad rows quarantined in $path")
    }

    df.filter(not(filterCondition))
  }
}