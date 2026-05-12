package transform

import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class TransformAggregateSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  test("TransformAggregate computes configured metrics deterministically") {
    // Arrange
    val df = Seq(
      ("sales", "usa", 100.0, 1),
      ("sales", "usa", 150.0, 2),
      ("finance", "france", 200.0, 3)
    ).toDF("department", "country", "salary", "id")

    val config = TransformAggregate.AggregationConfig(
      enabled = true,
      groupByColumns = Seq("department", "country"),
      metrics = Seq(
        TransformAggregate.AggregationMetric("salary", "sum"),
        TransformAggregate.AggregationMetric("salary", "avg"),
        TransformAggregate.AggregationMetric("id", "count")
      ),
      suffixEnabled = true
    )

    // Act
    val result = TransformAggregate.aggregate(df, config)

    // Assert
    result.count() shouldEqual 2L
    val sales = result.filter(col("department") === "sales").head()
    sales.getAs[Double]("salary_sum") shouldEqual 250.0
    sales.getAs[Double]("salary_avg") shouldEqual 125.0
    sales.getAs[Long]("id_count") shouldEqual 2L
  }
}
