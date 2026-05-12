package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * QualityChecks — post-stage data quality gate.
 *
 * This object acts as a lightweight validation layer that runs after the
 * `clean` and `dedup` stages. Its purpose is to surface data quality
 * problems that survive cleaning — not to fix them, but to report them so
 * the pipeline can make a decision (warn, fail-fast, or continue).
 *
 * Three categories of checks are performed:
 *
 *   1. **Critical-column nulls** — even after cleaning, a critical column
 *      may still have nulls if rows were not quarantined. Counts nulls per
 *      column and emits an issue string per offending column.
 *
 *   2. **Row null-ratio** — counts rows where the proportion of null columns
 *      exceeds `maxNullRatioPerRow`. This complements the per-column check
 *      by catching rows that are technically not null on any single critical
 *      column but are mostly empty overall.
 *
 *   3. **Duplicate business-key detection** — verifies that the deduplication
 *      stage was effective by checking whether any duplicates remain on the
 *      configured key columns. A non-zero count here indicates a deduplication
 *      misconfiguration or a key collision not covered by `keyColumns`.
 *
 * Issue strings follow the format `"type:detail:count"`, making them easy
 * to parse or display without a dedicated model class.
 *
 * These checks are intentionally read-only — they produce no side effects and
 * do not modify the DataFrame. The caller ([[Main.EtlPipeline.runQualityChecks]])
 * decides what to do with the returned issue list.
 */
object QualityChecks {

  /**
   * Configuration for a single quality-check invocation.
   *
   * @param enabled             If false, skip all checks and return an empty list.
   * @param criticalColumns     Column names that must be non-null after cleaning.
   * @param maxNullRatioPerRow  Threshold (0.0–1.0) for the per-row null ratio check.
   *                            A value of 0.7 flags rows where more than 70% of
   *                            columns are null. Set to 1.0 to disable.
   * @param deduplicationKeys   Business-key columns used to detect remaining duplicates.
   *                            Should match the `key-columns` setting in the dedup config.
   */
  case class QualityConfig(
    enabled:            Boolean,
    criticalColumns:    Seq[String],
    maxNullRatioPerRow: Double,
    deduplicationKeys:  Seq[String]
  )

  /**
   * Validate a DataFrame against the configured quality rules.
   *
   * This method triggers multiple Spark Actions (`.count()`, `.filter().count()`).
   * It is intentionally called between heavy transformation stages where the
   * DataFrame is already partially materialised, so the overhead is acceptable
   * relative to the value of catching quality regressions early.
   *
   * @param df      DataFrame to validate (typically post-clean or post-dedup).
   * @param config  [[QualityConfig]] with enabled checks and thresholds.
   * @return        A list of human-readable issue strings. An empty list means
   *                all checks passed. Non-empty means one or more violations were
   *                detected; the caller decides severity.
   */
  def validate(df: DataFrame, config: QualityConfig): Seq[String] = {
    if (!config.enabled) return Seq.empty

    var issues = Seq.empty[String]

    // ── Check 1: Critical columns must not contain nulls ──────────────────────
    // Only validate columns that actually exist in the DataFrame schema;
    // config may reference columns that were not present in all sources.
    val criticalExisting = config.criticalColumns.filter(df.columns.contains)
    criticalExisting.foreach { c =>
      val nullCount = df.filter(col(c).isNull).count()
      if (nullCount > 0) {
        issues = issues :+ s"critical-column-null:$c:$nullCount"
      }
    }

    // ── Check 2: Row-level null ratio ────────────────────────────────────────
    // Build a single expression that counts nulls per row as a ratio [0.0, 1.0].
    // Using threshold < 1.0 guard to avoid unnecessary computation when disabled.
    if (config.maxNullRatioPerRow < 1.0 && df.columns.nonEmpty) {
      val totalCols = df.columns.length
      val nullExpr  = df.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
      val exceeded  = df.filter((nullExpr / lit(totalCols)) > lit(config.maxNullRatioPerRow)).count()
      if (exceeded > 0) issues = issues :+ s"row-null-ratio-exceeded:$exceeded"
    }

    // ── Check 3: Remaining duplicates on deduplication keys ──────────────────
    // dropDuplicates triggers a shuffle; only run when keys are configured and
    // the columns exist in the DataFrame.
    val dedupKeys = config.deduplicationKeys.filter(df.columns.contains)
    if (dedupKeys.nonEmpty) {
      val dupCount = df.count() - df.dropDuplicates(dedupKeys).count()
      if (dupCount > 0) issues = issues :+ s"duplicate-business-keys:$dupCount"
    }

    issues
  }
}
