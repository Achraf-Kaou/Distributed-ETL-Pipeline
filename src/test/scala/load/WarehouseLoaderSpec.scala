package load

import config.PipelineConfig.{UpsertConfig, WarehouseConfig, WarehouseTableConfig, WarehouseTargetConfig}
import java.nio.file.Files
import java.sql.DriverManager
import org.scalatest.matchers.should.Matchers
import support.SparkSessionTestWrapper

class WarehouseLoaderSpec extends SparkSessionTestWrapper with Matchers {

  import spark.implicits._

  private val warehouseConfig = WarehouseConfig(
    enabled = true,
    dimensions = Seq(
      WarehouseTableConfig("dim_department", "department_sk", Seq("department_name")),
      WarehouseTableConfig("dim_employee", "employee_sk", Seq("employee_bk")),
      WarehouseTableConfig("dim_date", "date_sk", Seq("date_value"))
    ),
    fact = WarehouseTableConfig("fact_employee_metrics", "fact_id", Seq("employee_sk", "department_sk", "date_sk"))
  )

  test("WarehouseLoader writes and upserts star schema tables into SQLite") {
    // Arrange
    val tempDir = Files.createTempDirectory("warehouse-loader")
    val dbFile = tempDir.resolve("warehouse.db")
    val target = WarehouseTargetConfig(
      enabled = true,
      dbType = "sqlite",
      host = "",
      port = 0,
      database = "",
      user = "",
      password = "",
      sqliteFilePath = dbFile.toString,
      schema = "main"
    )
    val upsert = UpsertConfig(enabled = true, stagingPrefix = "stg_", batchSize = 100)

    def buildStar(salary: Double) = StarSchemaBuilder.build(
      spark,
      Seq((1, "amelia.hart@northwind.com", "Amelia Hart", 42, "active", "usa", "new york", "sales", "2025-01-15", salary))
        .toDF("id", "email", "full_name", "age", "status", "country", "city", "department", "join_date", "salary"),
      warehouseConfig
    )

    // Act
    WarehouseLoader.loadStarSchema(buildStar(91000.0), warehouseConfig, target, upsert)
    WarehouseLoader.loadStarSchema(buildStar(92000.0), warehouseConfig, target, upsert)

    // Assert
    Class.forName("org.sqlite.JDBC")
    val connection = DriverManager.getConnection(s"jdbc:sqlite:${dbFile.toAbsolutePath}")
    try {
      val statement = connection.createStatement()
      val factResult = statement.executeQuery("SELECT salary FROM fact_employee_metrics")
      factResult.next() shouldBe true
      factResult.getDouble("salary") shouldEqual 92000.0
      factResult.next() shouldBe false
      factResult.close()
      statement.close()
    } finally {
      connection.close()
    }
  }
}
