package load

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import config.PipelineConfig.WarehouseConfig

/**
 * StarSchemaBuilder — constructs a star-schema warehouse model from a flat DataFrame.
 *
 * This object implements the "build_star" pipeline stage. It takes the fully
 * enriched, deduplicated, and joined employee DataFrame produced by the upstream
 * transformation stages and materialises it into the classic data warehouse pattern:
 * one fact table + multiple conformed dimension tables.
 *
 * == Star Schema Design ==
 *
 * {{{
 *   dim_department   ──────────────────────────────────────────────┐
 *     department_sk (PK, surrogate)                                │
 *     department_name                                              │
 *                                                                  │
 *   dim_employee     ────────────────────────────────┐            │
 *     employee_sk (PK, surrogate)                    │            │
 *     employee_bk  (business key: email or id)       │            │
 *     full_name, email, age, status, country, city   │            │
 *                                                    │            │
 *   dim_date         ──────────────────────────────┐ │            │
 *     date_sk (PK, integer YYYYMMDD)               │ │            │
 *     date_value, year, month, day                 │ │            │
 *                                                  ▼ ▼            ▼
 *                         fact_employee_metrics
 *                           employee_sk  (FK → dim_employee)
 *                           department_sk (FK → dim_department)
 *                           date_sk       (FK → dim_date)
 *                           salary        (additive measure)
 *                           employee_count (additive measure: always 1 per row)
 * }}}
 *
 * == Surrogate Key Generation ==
 *
 * Surrogate keys are generated using `dense_rank()` window functions over an
 * `orderBy` on the business key column. This approach is:
 *   - **Deterministic**: the same input always produces the same surrogate key.
 *   - **Compact**: keys are sequential integers starting at 1, with no gaps.
 *   - **Idempotent**: re-running the pipeline on the same data yields the same keys.
 *
 * `date_sk` uses a special format: `date_format(date_value, "yyyyMMdd").cast("int")`
 * (e.g. `20240115` for 2024-01-15). This is a widely used convention in data
 * warehousing because it makes date arithmetic readable and the key self-descriptive.
 *
 * == Column Discovery ==
 *
 * The builder uses [[choose]] to find dimension-relevant columns by trying a
 * prioritised list of candidate names. This makes the builder tolerant of schema
 * variations across sources (e.g. `"email"` vs `"id"` as the employee business key).
 * If a required column is missing entirely, the corresponding dimension is returned
 * as an empty DataFrame and a warning is emitted — the pipeline does not abort.
 *
 * == Fact Table Construction ==
 *
 * The fact table is built by joining the enriched base DataFrame against the three
 * dimension tables on their business keys, then projecting only the surrogate keys
 * and measures. Rows where any surrogate key is null (i.e. the business key was
 * not found in the corresponding dimension) are excluded — they represent data
 * quality issues that should have been caught upstream.
 *
 * == Spark Considerations ==
 *
 * `dense_rank()` is a Window function that requires a full shuffle and sort of the
 * data partitioned by the sort key. For the scale of this project (hundreds to
 * thousands of rows), this is negligible. For large datasets, consider pre-sorting
 * or using a monotonically_increasing_id() approach with a separate key mapping table.
 */
object StarSchemaBuilder {

  /**
   * Container for the four DataFrames that form the star schema.
   *
   * @param dimDepartment       Dimension table: one row per unique department.
   * @param dimEmployee         Dimension table: one row per unique employee (by business key).
   * @param dimDate             Dimension table: one row per unique join date.
   * @param factEmployeeMetrics Fact table: one row per employee per department per date,
   *                            with salary and employee_count as additive measures.
   */
  case class StarSchemaResult(
    dimDepartment:       DataFrame,
    dimEmployee:         DataFrame,
    dimDate:             DataFrame,
    factEmployeeMetrics: DataFrame
  )

  /**
   * Build all star-schema tables from the enriched employee DataFrame.
   *
   * @param spark          Active [[SparkSession]] (needed for `spark.implicits` and
   *                       `spark.emptyDataFrame` fallbacks).
   * @param df             Enriched, deduplicated, joined DataFrame from the pipeline.
   * @param warehouseConfig Warehouse configuration — currently used for future extension
   *                        (dimension names, business key overrides from config).
   * @param warnLogger     Callback for warning messages when expected columns are absent.
   *                       Defaults to `println("[WARN] ...")`. Injected by the pipeline as
   *                       `logger.warn` so warnings flow through the structured log system.
   * @return               [[StarSchemaResult]] containing all four DataFrames.
   *                       Dimensions with missing source columns are empty DataFrames.
   */
  def build(
    spark:           SparkSession,
    df:              DataFrame,
    warehouseConfig: WarehouseConfig,
    warnLogger:      String => Unit = (m: String) => println(s"[WARN] $m")
  ): StarSchemaResult = {
    import spark.implicits._

    // ── Column discovery: try each candidate in priority order ───────────────
    val departmentCol = choose(df.columns, Seq("department", "dim_dept_name"))
    val employeeIdCol = choose(df.columns, Seq("email", "id"))
    val joinDateCol   = choose(df.columns, Seq("join_date"))

    if (departmentCol.isEmpty) warnLogger("Star schema: missing department column, dim_department will be empty")
    if (employeeIdCol.isEmpty) warnLogger("Star schema: missing employee business key column, dim_employee will be empty")
    if (joinDateCol.isEmpty)   warnLogger("Star schema: missing join_date column, dim_date will be empty")

    // ── dim_department ────────────────────────────────────────────────────────
    // One row per unique department name; surrogate key via dense_rank.
    val dimDepartment = if (departmentCol.nonEmpty) {
      df.select(col(departmentCol).as("department_name"))
        .where(col("department_name").isNotNull)
        .dropDuplicates()
        .withColumn("department_sk", dense_rank().over(Window.orderBy(col("department_name"))))
    } else spark.emptyDataFrame

    // ── dim_employee ─────────────────────────────────────────────────────────
    // One row per unique employee (deduplicated on business key).
    // Carries descriptive attributes used by analysts for slicing/filtering.
    val dimEmployee = if (employeeIdCol.nonEmpty) {
      val selectedCols = Seq("full_name", "email", "age", "status", "country", "city")
        .filter(df.columns.contains)
        .map(col)
      df.select((col(employeeIdCol).as("employee_bk") +: selectedCols): _*)
        .where(col("employee_bk").isNotNull)
        .dropDuplicates("employee_bk")
        .withColumn("employee_sk", dense_rank().over(Window.orderBy(col("employee_bk"))))
    } else spark.emptyDataFrame

    // ── dim_date ─────────────────────────────────────────────────────────────
    // One row per unique calendar date derived from the employee join_date.
    // date_sk uses YYYYMMDD integer format — a DWH convention for human-readable,
    // sortable, self-descriptive date surrogate keys.
    val dimDate = if (joinDateCol.nonEmpty) {
      df.select(to_date(col(joinDateCol)).as("date_value"))
        .where(col("date_value").isNotNull)
        .dropDuplicates()
        .withColumn("date_sk",  date_format(col("date_value"), "yyyyMMdd").cast("int"))
        .withColumn("year",     year(col("date_value")))
        .withColumn("month",    month(col("date_value")))
        .withColumn("day",      dayofmonth(col("date_value")))
    } else spark.emptyDataFrame

    // ── fact_employee_metrics ────────────────────────────────────────────────
    // Join base DataFrame against each dimension to resolve business keys →
    // surrogate keys, then project only the foreign keys and measures.
    // Left joins preserve base rows even when a dimension key is missing —
    // the final WHERE clause then filters out any incomplete fact rows.
    val base = df
      .withColumn("employee_bk",    if (employeeIdCol.nonEmpty) col(employeeIdCol) else lit(null))
      .withColumn("department_name", if (departmentCol.nonEmpty) col(departmentCol) else lit(null))
      .withColumn("date_value",      if (joinDateCol.nonEmpty) to_date(col(joinDateCol)) else lit(null).cast("date"))
      .withColumn("salary_value",    if (df.columns.contains("salary")) col("salary").cast("double") else lit(0.0))

    val factEmployeeMetrics = base
      .join(dimEmployee.select("employee_sk",   "employee_bk"),   Seq("employee_bk"),   "left")
      .join(dimDepartment.select("department_sk","department_name"),Seq("department_name"),"left")
      .join(dimDate.select("date_sk",           "date_value"),    Seq("date_value"),    "left")
      .select(
        col("employee_sk"),
        col("department_sk"),
        col("date_sk"),
        col("salary_value").as("salary"),
        lit(1L).as("employee_count")   // Additive headcount measure: always 1 per row.
      )
      // Exclude rows where any FK is null — these are data quality issues
      // that should have been caught by the quarantine stage upstream.
      .where(col("employee_sk").isNotNull && col("department_sk").isNotNull && col("date_sk").isNotNull)

    StarSchemaResult(dimDepartment, dimEmployee, dimDate, factEmployeeMetrics)
  }

  /**
   * Find the first column name from `candidates` that exists in `columns`.
   *
   * Used for resilient column discovery: when different source systems name the
   * same concept differently (e.g. `"email"` vs `"id"` as the employee key),
   * the priority list lets the builder degrade gracefully rather than failing hard.
   *
   * @param columns    Available column names in the DataFrame.
   * @param candidates Ordered list of candidate column names (first match wins).
   * @return           The first matching column name, or `""` if none match.
   */
  private def choose(columns: Seq[String], candidates: Seq[String]): String =
    candidates.find(columns.contains).getOrElse("")
}
