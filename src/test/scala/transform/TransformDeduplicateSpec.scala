package transform

import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class TransformDeduplicateSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  test("TransformDeduplicate keeps the most recent row and honors source priority") {
    // Arrange
    val df = Seq(
      (1, "amelia.hart@northwind.com", "2024-01-01", "mysql", 88000.0),
      (2, "amelia.hart@northwind.com", "2025-01-15", "postgres", 91000.0),
      (3, "amelia.hart@northwind.com", "2025-01-15", "sqlite", 87000.0),
      (4, "zara.ali@corp.example", "2022-07-19", "mysql", 83000.0)
    ).toDF("id", "email", "join_date", "__source", "salary")

    val config = TransformDeduplicate.DeduplicateConfig(
      keyColumns = Seq("email"),
      recencyColumn = Some("join_date"),
      sourcePriority = Map("postgres" -> 1, "mysql" -> 2, "sqlite" -> 3),
      dropSourceCol = false,
      verbose = false
    )

    // Act
    val result = TransformDeduplicate.deduplicate(df, config)

    // Assert
    result.count() shouldEqual 2L
    val survivor = result.filter(col("email") === "amelia.hart@northwind.com").head()
    survivor.getAs[Double]("salary") shouldEqual 91000.0
    survivor.getAs[String]("__source") shouldEqual "postgres"
  }

  test("TransformDeduplicate rejects missing key columns") {
    // Arrange
    val df = Seq((1, "alice@example.com")).toDF("id", "email")

    // Act / Assert
    val error = intercept[IllegalArgumentException] {
      TransformDeduplicate.deduplicate(
        df,
        TransformDeduplicate.DeduplicateConfig(keyColumns = Seq("missing_key"), verbose = false)
      )
    }

    error.getMessage should include ("missing")
  }
}
