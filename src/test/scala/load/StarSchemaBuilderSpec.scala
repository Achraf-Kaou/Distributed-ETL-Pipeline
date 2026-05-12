package load

import config.PipelineConfig
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class StarSchemaBuilderSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  test("StarSchemaBuilder builds dimension and fact tables from employee data") {
    // Arrange
    val df = Seq(
      (1, "amelia.hart@northwind.com", "Amelia Hart", 42, "active", "usa", "new york", "sales", "2025-01-15", 91000.0),
      (2, "leila.benali@corp.example", "Leila Benali", 34, "active", "france", "paris", "finance", "2024-03-05", 79000.0)
    ).toDF("id", "email", "full_name", "age", "status", "country", "city", "department", "join_date", "salary")

    val warehouseConfig = PipelineConfig.load("application.conf").warehouse

    // Act
    val star = StarSchemaBuilder.build(spark, df, warehouseConfig)

    // Assert
    star.dimDepartment.count() shouldEqual 2L
    star.dimEmployee.count() shouldEqual 2L
    star.dimDate.count() shouldEqual 2L
    star.factEmployeeMetrics.count() shouldEqual 2L
  }
}
