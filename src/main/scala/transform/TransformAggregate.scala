package transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * TransformAggregate — configuration-driven GROUP BY aggregation step.
 *
 * This object sits at the end of the transformation chain, after cleaning,
 * deduplication, and joining. Its purpose is to collapse the enriched row-level
 * employee DataFrame into summary metrics per business dimension (e.g. department,
 * country), which is the typical shape data consumers (BI tools, dashboards) need.
 *
 * Design decisions:
 *
 *   - **Config-driven**: grouping columns and metric functions are declared in
 *     `application.conf` under `etl.transform.aggregation`, so new aggregations
 *     can be added without touching Scala code.
 *
 *   - **Suffix naming**: when `suffixEnabled = true`, output columns are named
 *     `<column>_<function>` (e.g. `salary_sum`, `salary_avg`). This avoids column
 *     name collisions when aggregating the same column with multiple functions and
 *     makes the metric semantics self-documenting.
 *
 *   - **Early exit**: if aggregation is disabled, or no groupBy/metrics are
 *     configured, the input DataFrame is returned unchanged. This allows the
 *     pipeline to run in "raw" mode without aggregation.
 *
 * Spark performance note:
 *   `groupBy(...).agg(...)` triggers a shuffle — data is redistributed across
 *   partitions so all rows sharing the same key land on the same executor.
 *   The shuffle size is controlled by `spark.sql.shuffle.partitions` (set to 8
 *   in the default config). For small datasets like this one, a lower value
 *   reduces overhead; increase it for production-scale data.
 */
object TransformAggregate {

  /**
   * Configuration for a single aggregation pass.
   *
   * @param enabled        If false, skip aggregation and return the input DataFrame unchanged.
   * @param groupByColumns Columns to group by. Example: `Seq("department", "country")`.
   * @param metrics        List of (column, function) pairs to compute. Multiple metrics
   *                       on the same column (e.g. sum + avg of salary) are fully supported.
   * @param suffixEnabled  If true, output columns are named `<column>_<function>`
   *                       (e.g. `salary_sum`). If false, the column retains its original name
   *                       — only safe when a column appears in a single metric.
   */
  case class AggregationConfig(
    enabled:        Boolean           = false,
    groupByColumns: Seq[String]       = Seq.empty,
    metrics:        Seq[AggregationMetric] = Seq.empty,
    suffixEnabled:  Boolean           = true
  )

  /**
   * A single metric to compute in the aggregation.
   *
   * @param column    Name of the column to aggregate, e.g. `"salary"` or `"id"`.
   * @param function  Aggregation function name (case-insensitive):
   *                  `"sum"`, `"avg"`, `"max"`, `"min"`, `"count"`.
   *                  Any unrecognised function defaults to `sum` — extend the match
   *                  in [[aggregate]] to add new functions.
   */
  case class AggregationMetric(
    column:   String,
    function: String
  )

  /**
   * Apply GROUP BY aggregation to the DataFrame according to [[AggregationConfig]].
   *
   * The method builds Spark Column expressions from the config, executes a single
   * `groupBy(...).agg(...)` call, and returns the aggregated DataFrame.
   *
   * Supported aggregation functions:
   * | Config value | Spark function      |
   * |--------------|---------------------|
   * | `"sum"`      | `functions.sum`     |
   * | `"avg"`      | `functions.avg`     |
   * | `"max"`      | `functions.max`     |
   * | `"min"`      | `functions.min`     |
   * | `"count"`    | `functions.count`   |
   * | (other)      | defaults to `sum`   |
   *
   * Example output with `group-by = ["department", "country"]`:
   * {{{
   *   department | country | salary_sum | salary_avg | id_count | age_avg
   *   IT         | France  | 117000.0   | 58500.0    | 2        | 31.0
   *   HR         | France  | 52000.0    | 52000.0    | 1        | 34.0
   * }}}
   *
   * @param df     Enriched, deduplicated, joined DataFrame from the upstream stage.
   * @param config [[AggregationConfig]] specifying grouping and metric definitions.
   * @return       Aggregated DataFrame, or the original `df` if aggregation is disabled.
   */
  def aggregate(df: DataFrame, config: AggregationConfig = AggregationConfig()): DataFrame = {
    if (!config.enabled || config.groupByColumns.isEmpty || config.metrics.isEmpty) {
      println("ℹ️ Aggregation disabled in configuration.")
      return df
    }

    println(s"\n📊 Aggregation Phase - Grouping by: ${config.groupByColumns.mkString(", ")}")

    // Build one Spark Column expression per configured metric.
    val aggExprs = config.metrics.map { metric =>
      val aggFunc = metric.function.toLowerCase match {
        case "sum"   => sum(col(metric.column))
        case "avg"   => avg(col(metric.column))
        case "max"   => max(col(metric.column))
        case "min"   => min(col(metric.column))
        case "count" => count(col(metric.column))
        case _       => sum(col(metric.column))  // Safe fallback — sum is always meaningful.
      }

      // Suffix the output column name to make it self-describing and collision-safe.
      val alias = if (config.suffixEnabled) s"${metric.column}_${metric.function}" else metric.column
      aggFunc.as(alias)
    }

    // groupBy triggers a shuffle; agg is evaluated in the same stage.
    val result = df
      .groupBy(config.groupByColumns.map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)

    println(s"   Aggregated into ${result.count()} rows")
    result
  }
}