package extract

import org.apache.spark.sql.functions.col
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class ExtractFlatFilesSpec extends SparkSessionTestWrapper with Matchers {

  test("ExtractFlatFiles reads enriched CSV employee data") {
    // Arrange
    val path = repoPath("data/raw/employees_source1.csv")

    // Act
    val df = ExtractFlatFiles.read(spark, path)

    // Assert
    df.count() shouldEqual 8L
    df.columns should contain allOf ("id", "full_name", "email", "join_date")
    df.filter(col("email") === "amelia.hart@northwind.com").count() shouldEqual 2L
  }

  test("ExtractFlatFiles explodes nested employee JSON payloads") {
    // Arrange
    val path = repoPath("data/raw/employees_complex.json")

    // Act
    val df = ExtractFlatFiles.read(spark, path)

    // Assert
    df.count() shouldEqual 4L
    df.columns should contain allOf ("personal_info", "employment", "address")
  }

  test("ExtractFlatFiles reads enterprise parquet datasets") {
    // Arrange
    val path = repoPath("data/raw/enterprise/parquet/products_catalog.parquet")

    // Act
    val df = ExtractFlatFiles.read(spark, path)

    // Assert
    df.count() shouldEqual 5L
    df.columns should contain allOf ("product_sku", "unit_price", "updated_at")
    df.filter(col("product_sku") === "SKU-003").count() shouldEqual 2L
  }
}
