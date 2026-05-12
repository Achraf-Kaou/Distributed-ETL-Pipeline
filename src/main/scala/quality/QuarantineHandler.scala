package quality

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, not}
import logging.PipelineLogger
import java.time.Instant

class QuarantineHandler(spark: SparkSession, logger: PipelineLogger) {

  private val quarantineBasePath = "output/quarantine"

  /**
   * Quarantine bad rows and return clean DataFrame
   */
  def quarantineAndClean(
    df: DataFrame,
    runId: String,
    stage: String,
    criticalColumns: Seq[String],
    reason: String = "critical_nulls"
  ): DataFrame = {

    if (df.isEmpty) return df

    // Identify bad rows (example: critical columns are null)
    val badRowsCondition = criticalColumns
      .map(columnName => col(columnName).isNull)
      .reduceOption(_ || _)
      .getOrElse(col("dummy").isNull) // fallback

    val badRows = df.filter(badRowsCondition)

    val badCount = badRows.count()

    if (badCount > 0) {
      val quarantinePath = s"$quarantineBasePath/$runId/${stage}_$reason"

      badRows
        .withColumn("quarantine_reason", lit(reason))
        .withColumn("quarantine_timestamp", lit(Instant.now().toString))
        .write
        .mode(SaveMode.Append)
        .parquet(quarantinePath)

      logger.warn(s"QUARANTINE | $badCount rows moved to $quarantinePath (reason: $reason)")
    }

    // Return clean rows
    val cleanRows = df.filter(not(badRowsCondition))
    cleanRows
  }

  /**
   * Simple version for custom filtering
   */
  def quarantineCustom(
    df: DataFrame,
    runId: String,
    stage: String,
    filterCondition: org.apache.spark.sql.Column,
    reason: String
  ): DataFrame = {

    val badRows = df.filter(filterCondition)
    val badCount = badRows.count()

    if (badCount > 0) {
      val path = s"$quarantineBasePath/$runId/${stage}_$reason"
      badRows
        .withColumn("quarantine_reason", lit(reason))
        .withColumn("quarantine_timestamp", lit(Instant.now().toString))
        .write.mode(SaveMode.Append).parquet(path)

      logger.warn(s"QUARANTINE | $badCount bad rows quarantined in $path")
    }

    df.filter(not(filterCondition))
  }
}