package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * TransformJoin — generic, parameterized join step.
 *
 * Responsibilities:
 *   1. Join two DataFrames on single or composite keys
 *   2. Support all join types: inner, left, right, full outer
 *   3. Select/rename output columns explicitly (caller controls shape)
 *   4. Validate keys exist on both sides before joining (fail fast)
 *   5. Report join results: row counts before and after
 *
 * Design decisions:
 *   - Duplicate column handling is the CALLER's responsibility.
 *     If both sides have "created_at", rename one before calling join().
 *     This keeps TransformJoin focused and avoids silent data loss from
 *     automatic column dropping.
 *   - Join keys are passed as Seq[String] — works for 1 or many columns.
 *   - Output columns are opt-in: if selectColumns is empty, all columns
 *     are kept. This lets callers incrementally refine their output shape.
 *
 * Usage:
 *   val config = JoinConfig(
 *     joinType      = "left",
 *     keyColumns    = Seq("user_id"),
 *     selectColumns = Seq("user_id", "name", "email", "total_orders")
 *   )
 *   val result = TransformJoin.join(usersDf, ordersDf, config)
 */
object TransformJoin {

  // ─────────────────────────────────────────────────────────────────
  // Supported join types
  // ─────────────────────────────────────────────────────────────────

  sealed trait JoinType { val sparkValue: String }
  case object Inner     extends JoinType { val sparkValue = "inner"      }
  case object Left      extends JoinType { val sparkValue = "left"       }
  case object Right     extends JoinType { val sparkValue = "right"      }
  case object FullOuter extends JoinType { val sparkValue = "full"       }

  object JoinType {
    /**
     * Parse a join type from a plain string — case insensitive.
     * Accepted values: "inner", "left", "right", "full", "full_outer", "outer"
     */
    def fromString(s: String): JoinType = s.trim.toLowerCase match {
      case "inner"                    => Inner
      case "left" | "left_outer"      => Left
      case "right" | "right_outer"    => Right
      case "full" | "full_outer"
           | "outer"                  => FullOuter
      case other =>
        throw new IllegalArgumentException(
          s"Unknown join type: '$other'. " +
          s"Accepted: inner, left, right, full, full_outer, outer"
        )
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Configuration
  // ─────────────────────────────────────────────────────────────────

  /**
   * @param joinType      Type of join to perform.
   *                      Pass as string: "inner", "left", "right", "full".
   *                      Parsed into a typed JoinType internally.
   *
   * @param keyColumns    Column(s) to join on — must exist in BOTH DataFrames.
   *                      Single key  : Seq("user_id")
   *                      Composite   : Seq("name", "category")
   *                      NOTE: key columns appear ONCE in the output
   *                      (Spark's join-on-Seq behavior — no duplication).
   *
   * @param selectColumns Columns to keep in the output DataFrame.
   *                      If empty, ALL columns from both sides are kept.
   *                      Use this to trim the output to only what the
   *                      next pipeline step needs.
   *                      Example: Seq("user_id", "name", "total_orders")
   *
   * @param leftPrefix    Optional prefix to add to ALL left-side columns
   *                      before joining, to avoid name conflicts.
   *                      Example: "left_" → "created_at" becomes "left_created_at"
   *                      Only applied to non-key columns.
   *                      Default: "" (no prefix).
   *
   * @param rightPrefix   Same as leftPrefix but for the right side.
   *                      Example: "right_" → "created_at" becomes "right_created_at"
   *                      Default: "" (no prefix).
   *
   * @param verbose       Print join report. Default: true.
   */
  case class JoinConfig(
    joinType:      String,
    keyColumns:    Seq[String],
    selectColumns: Seq[String]  = Seq.empty,
    leftPrefix:    String       = "",
    rightPrefix:   String       = "",
    verbose:       Boolean      = true
  )

  // ─────────────────────────────────────────────────────────────────
  // Main entry point
  // ─────────────────────────────────────────────────────────────────

  /**
   * Join two DataFrames according to JoinConfig.
   *
   * Steps:
   *   1. Validate key columns exist on both sides
   *   2. Apply left/right prefixes to non-key columns (if configured)
   *   3. Execute the join
   *   4. Select output columns (if configured)
   *   5. Report
   *
   * @param left   left-side DataFrame
   * @param right  right-side DataFrame
   * @param config JoinConfig with all join parameters
   * @return       joined DataFrame
   */
  def join(
    left:   DataFrame,
    right:  DataFrame,
    config: JoinConfig
  ): DataFrame = {

    require(config.keyColumns.nonEmpty,
      "JoinConfig.keyColumns must not be empty.")

    val joinType = JoinType.fromString(config.joinType)

    // Step 1 — validate keys exist on both sides (fail fast)
    validateKeys(left, right, config.keyColumns)

    if (config.verbose) printHeader(left, right, config, joinType)

    // Step 2 — apply prefixes to non-key columns to avoid conflicts
    val (prefixedLeft, prefixedRight) = applyPrefixes(
      left, right, config.keyColumns, config.leftPrefix, config.rightPrefix
    )

    // Step 3 — execute the join
    // Joining on Seq[String] (not a Column expression) ensures key columns
    // appear only ONCE in the output — Spark's built-in deduplication for
    // equi-joins on named columns.
    var result = prefixedLeft.join(prefixedRight, config.keyColumns, joinType.sparkValue)

    // Step 4 — select output columns
    if (config.selectColumns.nonEmpty) {
      val existing = config.selectColumns.filter(result.columns.contains)
      val missing  = config.selectColumns.filterNot(result.columns.contains)

      if (missing.nonEmpty)
        println(
            s"""  ⚠️  Some selectColumns were not found and were skipped.
            |  Missing   : ${missing.mkString(", ")}
            |  Available : ${result.columns.mkString(", ")}
            |""".stripMargin
        )
        
      result = result.select(existing.map(col): _*)
    }

    if (config.verbose) printReport(left, right, result, config, joinType)

    result
  }

  // ─────────────────────────────────────────────────────────────────
  // Multi-join helper — chain multiple joins in one call
  // ─────────────────────────────────────────────────────────────────

  /**
   * Join a base DataFrame against multiple right-side DataFrames sequentially.
   *
   * Each join in the sequence uses its own JoinConfig — so you can mix
   * join types (e.g. inner join products, left join categories).
   *
   * WHY: In a star schema, the fact table is built by joining dimensions
   * one at a time. This helper makes that pipeline explicit and readable:
   *
   *   val factOrders = TransformJoin.joinAll(
   *     base = ordersDf,
   *     joins = Seq(
   *       (usersDf,    JoinConfig("left",  Seq("user_id"))),
   *       (productsDf, JoinConfig("inner", Seq("product_id")))
   *     )
   *   )
   *
   * @param base  the starting DataFrame (e.g. raw orders / fact table)
   * @param joins sequence of (rightDf, config) pairs applied left to right
   */
  def joinAll(
    base:  DataFrame,
    joins: Seq[(DataFrame, JoinConfig)]
  ): DataFrame = {
    joins.foldLeft(base) { case (accDf, (rightDf, config)) =>
      join(accDf, rightDf, config)
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Prefix helper — rename non-key columns to avoid conflicts
  // ─────────────────────────────────────────────────────────────────

  /**
   * Add a prefix to all non-key columns on left and/or right side.
   *
   * Key columns are left untouched — they are the join condition and
   * must match by name on both sides.
   *
   * Example:
   *   left has: user_id, name, created_at
   *   right has: user_id, order_count, created_at
   *   leftPrefix = "user_", rightPrefix = "order_"
   *   →  left becomes:  user_id, user_name, user_created_at
   *   →  right becomes: user_id, order_order_count, order_created_at
   *
   * After the join, selectColumns trims to only the columns you need.
   */
  private def applyPrefixes(
    left:       DataFrame,
    right:      DataFrame,
    keyColumns: Seq[String],
    leftPrefix:  String,
    rightPrefix: String
  ): (DataFrame, DataFrame) = {

    def addPrefix(df: DataFrame, prefix: String): DataFrame = {
      if (prefix.isEmpty) df
      else {
        df.columns.foldLeft(df) { (accDf, colName) =>
          if (keyColumns.contains(colName)) accDf  // never rename key columns
          else accDf.withColumnRenamed(colName, s"$prefix$colName")
        }
      }
    }

    (addPrefix(left, leftPrefix), addPrefix(right, rightPrefix))
  }

  // ─────────────────────────────────────────────────────────────────
  // Validation
  // ─────────────────────────────────────────────────────────────────

  /**
   * Fail fast if any key column is missing from either side.
   *
   * WHY: A missing key column causes Spark to throw a cryptic
   * AnalysisException at plan resolution time. An explicit check here
   * gives a clear, actionable error message before any computation starts.
   */
  private def validateKeys(
    left:       DataFrame,
    right:      DataFrame,
    keyColumns: Seq[String]
  ): Unit = {
    val missingLeft  = keyColumns.filterNot(left.columns.contains)
    val missingRight = keyColumns.filterNot(right.columns.contains)

    if (missingLeft.nonEmpty)
      throw new IllegalArgumentException(
        s"Key columns missing from LEFT DataFrame: ${missingLeft.mkString(", ")}\n" +
        s"Available columns: ${left.columns.mkString(", ")}"
      )

    if (missingRight.nonEmpty)
      throw new IllegalArgumentException(
        s"Key columns missing from RIGHT DataFrame: ${missingRight.mkString(", ")}\n" +
        s"Available columns: ${right.columns.mkString(", ")}"
      )
  }

  // ─────────────────────────────────────────────────────────────────
  // Reporting
  // ─────────────────────────────────────────────────────────────────

  private def printHeader(
    left:     DataFrame,
    right:    DataFrame,
    config:   JoinConfig,
    joinType: JoinType
  ): Unit = {
    println(s"\n${"=" * 70}")
    println(s"  🔗 TransformJoin — starting")
    println(s"${"=" * 70}")
    println(s"  Join type    : ${joinType.sparkValue.toUpperCase}")
    println(s"  Key columns  : ${config.keyColumns.mkString(", ")}")
    println(s"  Left  cols   : ${left.columns.mkString(", ")}")
    println(s"  Right cols   : ${right.columns.mkString(", ")}")
    println(s"  Left  rows   : ${left.count()}")
    println(s"  Right rows   : ${right.count()}")
    if (config.leftPrefix.nonEmpty)  println(s"  Left prefix  : '${config.leftPrefix}'")
    if (config.rightPrefix.nonEmpty) println(s"  Right prefix : '${config.rightPrefix}'")
  }

  private def printReport(
    left:     DataFrame,
    right:    DataFrame,
    result:   DataFrame,
    config:   JoinConfig,
    joinType: JoinType
  ): Unit = {
    val resultCount = result.count()
    println(s"\n  ✅ Join complete")
    println(s"  Left rows    : ${left.count()}")
    println(s"  Right rows   : ${right.count()}")
    println(s"  Output rows  : $resultCount")
    println(s"  Output cols  : ${result.columns.mkString(", ")}")
    println(s"${"=" * 70}\n")
  }
}