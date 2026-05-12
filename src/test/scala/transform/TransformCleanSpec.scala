package transform

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class TransformCleanSpec extends SparkSessionTestWrapper with Matchers {

  test("TransformClean normalizes strings, fills defaults, drops invalid rows, and casts deterministically") {
    // Arrange
    val schema = StructType(Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("Full Name", StringType, nullable = true),
      StructField("Email", StringType, nullable = true),
      StructField("Department", StringType, nullable = true),
      StructField("Country", StringType, nullable = true),
      StructField("Age", StringType, nullable = true),
      StructField("Join Date", StringType, nullable = true)
    ))
    val rows = Seq(
      Row(1, "  Alice Jones ", "ALICE@EXAMPLE.COM", "Sales", null, "35", "2024-01-15"),
      Row(2, null, "missing.name@example.com", "Sales", "USA", "29", "2024-01-16"),
      Row(3, "Bob Stone", "bob@example.com", "IT", null, null, "invalid-date"),
      Row(3, "Bob Stone", "bob@example.com", "IT", null, null, "invalid-date")
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val config = TransformClean.CleanConfig(
      criticalColumns = Seq("id", "full_name", "email"),
      fillValues = Map("country" -> "unknown", "age" -> "0"),
      castColumns = Map("age" -> IntegerType, "join_date" -> DateType),
      stringColumns = Seq("full_name", "email", "department", "country"),
      normalizeColNames = true,
      dropFullDuplicates = true,
      dropRowsWithNullsThreshold = 0.8,
      verbose = false
    )

    // Act
    val result = TransformClean.clean(df, config)
    val data = result.orderBy("id").collect()

    // Assert
    result.count() shouldEqual 2L
    result.columns should contain allOf ("id", "full_name", "email", "department", "country", "age", "join_date")
    data.head.getAs[String]("full_name") shouldEqual "alice jones"
    data.head.getAs[String]("country") shouldEqual "unknown"
    data(1).getAs[Int]("age") shouldEqual 0
    data(1).isNullAt(data(1).fieldIndex("join_date")) shouldBe true
  }
}
