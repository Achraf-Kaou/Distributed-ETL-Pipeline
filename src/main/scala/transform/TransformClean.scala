package transform

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TransformClean {

  /**
   * @param criticalColumns   Columns that MUST be non-null.
   *                          Rows with a null in any of these are dropped.
   * @param fillValues        Non-critical columns → default fill value.
   *                          Key = column name, Value = fill string
   *                          (Spark will cast to the column's actual type).
   *                          Example: Map("country" -> "Unknown", "age" -> "0")
   * @param castColumns       Explicit type casts to apply after filling nulls.
   *                          Example: Map("age" -> IntegerType, "price" -> DoubleType)
   * @param stringColumns     Columns to normalize: trim whitespace + lowercase.
   *                          Apply only to columns that are semantic strings
   *                          (NOT ids, codes, dates).
   * @param normalizeColNames If true, rename all columns to snake_case.
   *                          Default: true.
   * @param dropFullDuplicates If true, drop exact duplicate rows (all columns equal).
   *                          Default: true.
   * @param dropRowsWithNullsThreshold If > 0, drop rows where the percentage of nulls
   *                                 across all columns exceeds this threshold.
   *                                Example: 0.5 means drop rows with more than 50% nulls.
   * @param verbose           If true, print a detailed cleaning report.
   *                          Default: true.
   */

  case class CleanConfig(
    criticalColumns:            Seq[String]                 = Seq.empty,
    fillValues:                 Map[String, String]         = Map.empty,
    castColumns:                Map[String, DataType]       = Map.empty,
    stringColumns:              Seq[String]                 = Seq.empty,
    normalizeColNames:          Boolean                     = true,
    dropFullDuplicates:         Boolean                     = true,
    dropRowsWithNullsThreshold: Double                      = 0.0,
    verbose:                    Boolean                     = true
  )

  /**
   * @param df     raw input DataFrame (from Extract phase)
   * @param config CleanConfig with user-defined parameters
   * @return       cleaned DataFrame
   * 
   */
  def clean(df: DataFrame, config: CleanConfig = CleanConfig()): DataFrame = {

    val initialCount = df.count()
    if (config.verbose) printHeader(df, initialCount)

    var result = df

    // Step 1 — normalize column names to snake_case
    if (config.normalizeColNames) {
      result = normalizeColumnNames(result)
      // Remap config column references to normalized names
      // (user may have passed "First Name" — we handle both)
    }

    // Step 2 — normalize string values (trim + lowercase)
    if (config.stringColumns.nonEmpty) {
      result = normalizeStringValues(result, config.stringColumns.map(toSnakeCase))
    }

    // Step 3 — drop rows where critical columns are null
    if (config.criticalColumns.nonEmpty) {
      result = dropNullsInCritical(result, config.criticalColumns.map(toSnakeCase))
    }

    // Step 4 — drop rows with too many nulls (optional)
    if (config.dropRowsWithNullsThreshold > 0) {
      result = dropRowsWithTooManyNulls(result, config.dropRowsWithNullsThreshold)
    }

    // Step 5 — fill nulls in non-critical columns with defaults
    if (config.fillValues.nonEmpty) {
      result = fillNonCriticalNulls(result, config.fillValues.map {
        case (k, v) => toSnakeCase(k) -> v
      })
    }

    // Step 6 — cast columns to target types
    if (config.castColumns.nonEmpty) {
      result = castColumnTypes(result, config.castColumns.map {
        case (k, v) => toSnakeCase(k) -> v
      })
    }

    // Step 7 — drop exact duplicate rows
    if (config.dropFullDuplicates) {
      result = dropDuplicateRows(result)
    }

    if (config.verbose) printReport(df, result, initialCount)

    result
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 1 — Column name normalization
  // ─────────────────────────────────────────────────────────────────

  /**
   * Rename all columns to snake_case.
   *
   * Rules:
   *   - spaces and hyphens → underscore
   *   - CamelCase → camel_case
   *   - leading/trailing underscores removed
   *   - consecutive underscores collapsed
   *
   * Examples:
   *   "First Name"   → "first_name"
   *   "productID"    → "product_i_d"   (intentional — IDs should be renamed manually)
   *   "OrderDate"    → "order_date"
   *   "  country  "  → "country"
   */
  def normalizeColumnNames(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df) { (accDf, originalName) =>
      val normalized = toSnakeCase(originalName)
      if (normalized != originalName)
        accDf.withColumnRenamed(originalName, normalized)
      else
        accDf
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 2 — String value normalization
  // ─────────────────────────────────────────────────────────────────

  /**
   * For each column in stringColumns:
   *   - trim leading/trailing whitespace
   *   - lowercase the entire value
   *   - replace empty strings with null (so null handling catches them)
   *
   * WHY lowercase: inconsistent casing ("France", "france", "FRANCE") causes
   * GROUP BY and JOIN mismatches that are extremely hard to debug downstream.
   */
  def normalizeStringValues(df: DataFrame, stringColumns: Seq[String]): DataFrame = {
    val existingCols = stringColumns.filter(df.columns.contains)

    existingCols.foldLeft(df) { (accDf, colName) =>
      accDf.withColumn(
        colName,
        when(
          trim(lower(col(colName))) === "",
          lit(null).cast(StringType)
        ).otherwise(
          trim(lower(col(colName)))
        )
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 3 — Drop rows with nulls in critical columns
  // ─────────────────────────────────────────────────────────────────

  /**
   * Drop any row where at least one critical column is null.
   *
   * WHY: Critical columns (typically PKs, FKs, business keys like email)
   * cannot be filled with a default — a row without them is meaningless
   * in a relational star schema and would corrupt JOIN results.
   */
  def dropNullsInCritical(df: DataFrame, criticalColumns: Seq[String]): DataFrame = {
    val existingCols = criticalColumns.filter(df.columns.contains)
    if (existingCols.isEmpty) df else df.na.drop(how = "any", cols = existingCols)
  }
  // ─────────────────────────────────────────────────────────────────
  // Step 4 — Drop rows with too many nulls (optional)
  // ─────────────────────────────────────────────────────────────────

  /**
    * Drop rows where the percentage of nulls across all columns exceeds the given threshold.
    * Example: if threshold = 0.5, drop rows with more than 50% nulls.
    * WHY: Some rows may be mostly empty (e.g. only 1 out of 10 columns filled). Depending on the use case, it may be better to drop these rows entirely rather than trying to fill them with defaults, which could introduce noise. This step is optional and controlled by the dropRowsWithNullsThreshold parameter in CleanConfig.
    * Note: This is a simple heuristic. More sophisticated approaches could consider column importance or use machine learning to predict missing values, but this provides a straightforward way to filter out very incomplete rows without needing complex logic.
    *
    * @param df
    * @param threshold
    */
    def dropRowsWithTooManyNulls(df: DataFrame, threshold: Double): DataFrame = {
    val totalCols = df.columns.length
    val rowNullCount = df.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
    df.filter(rowNullCount / lit(totalCols) < lit(threshold))
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 5 — Fill nulls in non-critical columns
  // ─────────────────────────────────────────────────────────────────

  /**
   * Fill nulls in non-critical columns with user-supplied defaults.
   *
   * The fill value is always passed as String; Spark will cast it to the
   * column's actual DataType automatically.
   *
   * Example config:
   *   Map("country" -> "unknown", "age" -> "0", "category" -> "uncategorized")
   */
  def fillNonCriticalNulls(df: DataFrame, fillValues: Map[String, String]): DataFrame = {
    fillValues.foldLeft(df) { case (accDf, (colName, fillValue)) =>
      if (accDf.columns.contains(colName)) {
        val colType = accDf.schema(colName).dataType
        colType match {
          case StringType              => accDf.na.fill(fillValue, Seq(colName))
          case IntegerType | LongType  => accDf.na.fill(fillValue.toLong,   Seq(colName))
          case DoubleType | FloatType  => accDf.na.fill(fillValue.toDouble, Seq(colName))
          case _                       => accDf.na.fill(fillValue, Seq(colName))
        }
      } else {
        accDf
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 6 — Explicit type casting
  // ─────────────────────────────────────────────────────────────────

  /**
   * Cast columns to target DataTypes explicitly.
   *
   * WHY explicit casting instead of inferSchema:
   *   Spark's inferSchema reads a sample of rows and guesses. A column
   *   that looks like Long might need to be Integer for a JOIN key, or a
   *   String "2024-01-15" needs to be DateType for aggregations. Explicit
   *   casting makes the schema contract visible and deterministic.
   *
   * Example config:
   *   Map("age" -> IntegerType, "price" -> DoubleType, "created_at" -> TimestampType)
   */
  def castColumnTypes(df: DataFrame, castColumns: Map[String, DataType]): DataFrame = {
    castColumns.foldLeft(df) { case (accDf, (colName, targetType)) =>
      if (accDf.columns.contains(colName)) {
        accDf.withColumn(colName, col(colName).cast(targetType))
      } else {
        println(s"Cast skipped: column '$colName' not found in DataFrame")
        accDf
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Step 7 — Drop exact duplicate rows
  // ─────────────────────────────────────────────────────────────────

  /**
   * Drop rows where ALL column values are identical.
   *
   * This is structural deduplication (full-row equality).
   * Business-key deduplication (e.g. keep latest by email) is handled
   * in TransformDeduplicate, which uses Window functions.
   */
  def dropDuplicateRows(df: DataFrame): DataFrame = {
    df.dropDuplicates()
  }

  // ─────────────────────────────────────────────────────────────────
  // Utility — snake_case conversion
  // ─────────────────────────────────────────────────────────────────

  /**
   * Convert any string to snake_case.
   * Handles: spaces, hyphens, CamelCase, mixed cases.
   */
  def toSnakeCase(name: String): String = {
    name
      .trim
      .replaceAll("([A-Z])", "_$1")       // CamelCase → _Camel_Case
      .replaceAll("[\\s\\-]+", "_")        // spaces/hyphens → underscore
      .replaceAll("_+", "_")              // collapse multiple underscores
      .stripPrefix("_")                   // remove leading underscore
      .stripSuffix("_")                   // remove trailing underscore
      .toLowerCase
  }

  // ─────────────────────────────────────────────────────────────────
  // Reporting
  // ─────────────────────────────────────────────────────────────────

  private def printHeader(df: DataFrame, count: Long): Unit = {
    println(s"\n${"=" * 70}")
    println(s"  🧹 TransformClean — starting")
    println(s"${"=" * 70}")
    println(s"  Input schema:  ${df.columns.mkString(", ")}")
    println(s"  Input rows:    $count")
    println(s"  Null counts per column:")
    df.columns.foreach { c =>
      val nullCount = df.filter(col(c).isNull).count()
      if (nullCount > 0) println(s"    ⚠️  $c → $nullCount nulls")
    }
  }

  private def printReport(original: DataFrame, cleaned: DataFrame, originalCount: Long): Unit = {
    val cleanedCount = cleaned.count()
    val dropped      = originalCount - cleanedCount

    println(s"\n  ✅ Cleaning complete")
    println(s"  Rows before : $originalCount")
    println(s"  Rows after  : $cleanedCount")
    println(s"  Rows dropped: $dropped (${pct(dropped, originalCount)}% of input)")
    println(s"  Output cols : ${cleaned.columns.mkString(", ")}")
    println(s"${"=" * 70}\n")
  }

  private def pct(part: Long, total: Long): String =
    if (total == 0) "0.0" else f"${part.toDouble / total * 100}%.1f"
}