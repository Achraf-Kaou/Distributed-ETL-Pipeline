package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object QualityChecks {

  case class QualityConfig(
    enabled: Boolean,
    criticalColumns: Seq[String],
    maxNullRatioPerRow: Double,
    deduplicationKeys: Seq[String]
  )

  def validate(df: DataFrame, config: QualityConfig): Seq[String] = {
    if (!config.enabled) return Seq.empty

    var issues = Seq.empty[String]

    val criticalExisting = config.criticalColumns.filter(df.columns.contains)
    criticalExisting.foreach { c =>
      val nullCount = df.filter(col(c).isNull).count()
      if (nullCount > 0) {
        issues = issues :+ s"critical-column-null:$c:$nullCount"
      }
    }

    if (config.maxNullRatioPerRow < 1.0 && df.columns.nonEmpty) {
      val totalCols = df.columns.length
      val nullExpr = df.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
      val exceeded = df.filter((nullExpr / lit(totalCols)) > lit(config.maxNullRatioPerRow)).count()
      if (exceeded > 0) issues = issues :+ s"row-null-ratio-exceeded:$exceeded"
    }

    val dedupKeys = config.deduplicationKeys.filter(df.columns.contains)
    if (dedupKeys.nonEmpty) {
      val dupCount = df.count() - df.dropDuplicates(dedupKeys).count()
      if (dupCount > 0) issues = issues :+ s"duplicate-business-keys:$dupCount"
    }

    issues
  }
}
