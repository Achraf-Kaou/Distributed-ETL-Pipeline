package transform

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TransformAggregate {

  case class AggregationConfig(
    enabled: Boolean = false,
    groupByColumns: Seq[String] = Seq.empty,
    metrics: Seq[AggregationMetric] = Seq.empty,
    suffixEnabled: Boolean = true
  )

  case class AggregationMetric(
    column: String,
    function: String
  )

  def aggregate(df: DataFrame, config: AggregationConfig = AggregationConfig()): DataFrame = {
    if (!config.enabled || config.groupByColumns.isEmpty || config.metrics.isEmpty) {
      println("ℹ️ Aggregation disabled in configuration.")
      return df
    }

    println(s"\n📊 Aggregation Phase - Grouping by: ${config.groupByColumns.mkString(", ")}")

    val aggExprs = config.metrics.map { metric =>
      val aggFunc = metric.function.toLowerCase match {
        case "sum"   => sum(col(metric.column))
        case "avg"   => avg(col(metric.column))
        case "max"   => max(col(metric.column))
        case "min"   => min(col(metric.column))
        case "count" => count(col(metric.column))
        case _        => sum(col(metric.column))
      }

      val alias = if (config.suffixEnabled) s"${metric.column}_${metric.function}" else metric.column
      aggFunc.as(alias)
    }

    val result = df.groupBy(config.groupByColumns.map(col): _*)
                   .agg(aggExprs.head, aggExprs.tail: _*)

    println(s"   Aggregated into ${result.count()} rows")
    result
  }
}