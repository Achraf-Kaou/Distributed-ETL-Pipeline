package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.{map, lit, coalesce}

/**
 * TransformDeduplicate — business-key deduplication with survival strategy.
 *
 * Two deduplication modes, composable:
 *
 *   1. SIMPLE — dropDuplicates on a set of key columns.
 *               Fast. No ordering guarantee on which row survives.
 *               Use when all duplicates are truly identical on the key.
 *
 *   2. WINDOW — row_number() OVER (PARTITION BY <keys> ORDER BY <recency> DESC,
 *               <source_priority> ASC) → keep rank = 1.
 *               Deterministic. The "best" row always wins.
 *               Use when duplicates differ (different updated_at, different source).
 *
 * Survival strategy (as chosen):
 *   Primary   → most recent row wins  (orderBy recencyColumn DESC)
 *   Tiebreaker → source priority       (lower priority number = higher trust)
 *                e.g. postgres=1, mysql=2, api=3
 *
 * The source priority requires a __source column on the DataFrame.
 * TransformDeduplicate can inject it via DeduplicateConfig.sourceTag
 * before the union, or you can add it yourself upstream.
 *
 * Usage:
 *   val config = DeduplicateConfig(
 *     keyColumns      = Seq("email"),
 *     recencyColumn   = Some("created_at"),
 *     sourcePriority  = Map("postgres" -> 1, "mysql" -> 2, "api" -> 3),
 *     sourceTag       = Some("postgres")   // tag this DF before union
 *   )
 *   val deduped = TransformDeduplicate.deduplicate(df, config)
 */
object TransformDeduplicate {

  // ─────────────────────────────────────────────────────────────────
  // Configuration
  // ─────────────────────────────────────────────────────────────────

  /**
   * @param keyColumns      Business key columns that define uniqueness.
   *                        Example: Seq("email") for users,
   *                                 Seq("name", "category") for products.
   *
   * @param recencyColumn   Column used to rank rows by freshness (DESC).
   *                        Typically "created_at" or "updated_at".
   *                        If None, recency is ignored — source priority only.
   *
   * @param sourcePriority  Map of source tag → priority integer.
   *                        Lower number = higher trust = wins ties.
   *                        Example: Map("postgres" -> 1, "mysql" -> 2, "api" -> 3)
   *                        If empty, source priority is ignored — recency only.
   *
   * @param sourceTag       If set, adds a "__source" column with this value
   *                        to the DataFrame BEFORE deduplication.
   *                        Use this to tag each source DF before union:
   *                          pgDf.tag("postgres") ++ mysqlDf.tag("mysql")
   *                        If None, assumes "__source" already exists or
   *                        sourcePriority is empty.
   *
   * @param dropSourceCol   If true, drop the "__source" and "__priority"
   *                        helper columns from the final output.
   *                        Default: true (clean output).
   *
   * @param verbose         Print deduplication report. Default: true.
   */
  case class DeduplicateConfig(
    keyColumns:     Seq[String],
    recencyColumn:  Option[String]       = None,
    sourcePriority: Map[String, Int]     = Map.empty,
    sourceTag:      Option[String]       = None,
    dropSourceCol:  Boolean              = true,
    verbose:        Boolean              = true
  )

  // Internal column names — prefixed with __ to avoid collision
  private val COL_SOURCE   = "__source"
  private val COL_PRIORITY = "__priority"
  private val COL_RANK     = "__rank"

  // ─────────────────────────────────────────────────────────────────
  // Main entry point
  // ─────────────────────────────────────────────────────────────────

  /**
   * Deduplicate a DataFrame using Window-based survival strategy.
   *
   * Steps:
   *   1. Tag with source label (if sourceTag provided)
   *   2. Map source label → priority integer
   *   3. Build Window: PARTITION BY keys, ORDER BY recency DESC, priority ASC
   *   4. Assign row_number(), keep rank = 1
   *   5. Drop helper columns, report
   *
   * @param df     DataFrame to deduplicate (may be a union of multiple sources)
   * @param config DeduplicateConfig with survival strategy parameters
   */
  def deduplicate(df: DataFrame, config: DeduplicateConfig): DataFrame = {

    require(config.keyColumns.nonEmpty,
      "DeduplicateConfig.keyColumns must not be empty — define your business key.")
    validateColumns(df, config)

    val initialCount = df.count()
    if (config.verbose) printHeader(df, config, initialCount)

    var result = df

    // Step 1 — inject __source tag if requested
    result = config.sourceTag match {
      case Some(tag) => result.withColumn(COL_SOURCE, lit(tag))
      case None      => result
    }

    // Step 2 — map __source → __priority integer for ordering
    val hasPriority = config.sourcePriority.nonEmpty &&
                      result.columns.contains(COL_SOURCE)

    if (hasPriority) {
      val priorityMap = map(
        config.sourcePriority.toSeq.flatMap {
          case (k, v) => Seq(lit(k), lit(v))
        }: _*
      )
      result = result.withColumn(
        COL_PRIORITY,
        coalesce(
          priorityMap(col(COL_SOURCE)),
          lit(Int.MaxValue)
        ).cast(IntegerType)
      )
    }

    // Step 3 — build Window specification
    val windowPartition = Window.partitionBy(config.keyColumns.map(col): _*)

    val windowSpec = (config.recencyColumn, hasPriority) match {
      case (Some(recCol), true)  =>
        // Primary: most recent first; Tiebreaker: lower priority number wins
        windowPartition.orderBy(col(recCol).desc, col(COL_PRIORITY).asc)

      case (Some(recCol), false) =>
        // Recency only
        windowPartition.orderBy(col(recCol).desc)

      case (None, true)          =>
        // Source priority only
        windowPartition.orderBy(col(COL_PRIORITY).asc)

      case (None, false)         =>
        // No ordering specified — arbitrary survival (equivalent to dropDuplicates)
        windowPartition.orderBy(config.keyColumns.map(col): _*)
    }

    // Step 4 — assign rank, keep only rank = 1
    result = result
      .withColumn(COL_RANK, row_number().over(windowSpec))
      .filter(col(COL_RANK) === 1)
      .drop(COL_RANK)

    // Step 5 — optionally drop helper columns
    if (config.dropSourceCol) {
      val colsToDrop = Seq(COL_SOURCE, COL_PRIORITY).filter(result.columns.contains)
      result = result.drop(colsToDrop: _*)
    }

    if (config.verbose) printReport(initialCount, result.count(), config)

    result
  }

  private def validateColumns(df: DataFrame, config: DeduplicateConfig): Unit = {
    val missingKeys = config.keyColumns.distinct.filterNot(df.columns.contains)
    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(
        s"Deduplication key column(s) missing: ${missingKeys.mkString(", ")}. " +
        s"Available columns: ${df.columns.mkString(", ")}"
      )
    }

    val missingRecency = config.recencyColumn.filterNot(df.columns.contains)
    if (missingRecency.nonEmpty) {
      throw new IllegalArgumentException(
        s"Deduplication recency column '${missingRecency.get}' not found. " +
        s"Available columns: ${df.columns.mkString(", ")}"
      )
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Tag helper — call before union to label each source DataFrame
  // ─────────────────────────────────────────────────────────────────

  /**
   * Add a "__source" column to a DataFrame before unioning.
   *
   * Pattern:
   *   val tagged = Seq(
   *     TransformDeduplicate.tag(pgDf,    "postgres"),
   *     TransformDeduplicate.tag(mysqlDf, "mysql"),
   *     TransformDeduplicate.tag(apiDf,   "api")
   *   ).reduce(_ unionByName _)
   *
   *   val deduped = TransformDeduplicate.deduplicate(tagged, config)
   *
   * unionByName aligns columns by name, not position — safer than union()
   * when sources have slightly different column ordering.
   */
  def tag(df: DataFrame, sourceName: String): DataFrame =
    df.withColumn(COL_SOURCE, lit(sourceName))

  // ─────────────────────────────────────────────────────────────────
  // Simple mode — for when you just need fast exact-key dedup
  // ─────────────────────────────────────────────────────────────────

  /**
   * Simple deduplication: dropDuplicates on keyColumns only.
   * No ordering — Spark picks an arbitrary survivor.
   * Use only when all duplicates are guaranteed identical on non-key cols.
   *
   * @param df         input DataFrame
   * @param keyColumns columns that define uniqueness
   */
  def deduplicateSimple(df: DataFrame, keyColumns: Seq[String]): DataFrame = {
    require(keyColumns.nonEmpty, "keyColumns must not be empty.")
    val before = df.count()
    val result = df.dropDuplicates(keyColumns)
    println(s"  ✅ Simple dedup on [${keyColumns.mkString(", ")}]: $before → ${result.count()} rows")
    result
  }

  // ─────────────────────────────────────────────────────────────────
  // Reporting
  // ─────────────────────────────────────────────────────────────────

  private def printHeader(
    df: DataFrame,
    config: DeduplicateConfig,
    count: Long
  ): Unit = {
    println(s"\n${"=" * 70}")
    println(s"  🔁 TransformDeduplicate — starting")
    println(s"${"=" * 70}")
    println(s"  Input rows     : $count")
    println(s"  Key columns    : ${config.keyColumns.mkString(", ")}")
    println(s"  Recency column : ${config.recencyColumn.getOrElse("none")}")
    println(s"  Source priority: ${
      if (config.sourcePriority.isEmpty) "none"
      else config.sourcePriority.toSeq.sortBy(_._2).map { case (s, p) => s"$s=$p" }.mkString(", ")
    }")

    // Show duplicate count before dedup
    val dupCount = count - df.dropDuplicates(config.keyColumns).count()
    println(s"  Duplicate rows : $dupCount")
  }

  private def printReport(before: Long, after: Long, config: DeduplicateConfig): Unit = {
    val removed = before - after
    println(s"\n  ✅ Deduplication complete")
    println(s"  Rows before  : $before")
    println(s"  Rows after   : $after")
    println(s"  Rows removed : $removed (${pct(removed, before)}% of input)")
    println(s"  Strategy     : ${strategyLabel(config)}")
    println(s"${"=" * 70}\n")
  }

  private def strategyLabel(config: DeduplicateConfig): String =
    (config.recencyColumn.isDefined, config.sourcePriority.nonEmpty) match {
      case (true, true)  => "recency DESC + source priority ASC (tiebreaker)"
      case (true, false) => "recency DESC only"
      case (false, true) => "source priority ASC only"
      case _             => "arbitrary (no ordering)"
    }

  private def pct(part: Long, total: Long): String =
    if (total == 0) "0.0" else f"${part.toDouble / total * 100}%.1f"
}
