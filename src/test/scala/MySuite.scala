import config.PipelineConfig
import load.StarSchemaBuilder
import org.apache.spark.sql.SparkSession
import transform.QualityChecks

class MySuite extends munit.FunSuite {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("test")
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("configuration parses new sections") {
    val cfg = PipelineConfig.load("application.conf")
    assertEquals(cfg.spark.appName, "Distributed ETL Pipeline")
    assert(cfg.orchestration.stages.contains("build_star"))
    assertEquals(cfg.warehouse.fact.name, "fact_employee_metrics")
    assertEquals(cfg.load.warehouseTarget.dbType, "sqlite")
  }

  test("quality checks detect nulls and duplicates") {
    import spark.implicits._
    val df = Seq(
      (1, "a@x.com", "eng"),
      (2, "a@x.com", "eng"),
      (3, null.asInstanceOf[String], "hr")
    ).toDF("id", "email", "department")

    val issues = QualityChecks.validate(
      df,
      QualityChecks.QualityConfig(
        enabled = true,
        criticalColumns = Seq("email"),
        maxNullRatioPerRow = 1.0,
        deduplicationKeys = Seq("email")
      )
    )

    assert(issues.exists(_.startsWith("critical-column-null:email")))
    assert(issues.exists(_.startsWith("duplicate-business-keys")))
  }

  test("star schema builder creates expected tables") {
    import spark.implicits._
    val df = Seq(
      (1, "Alice", "alice@x.com", 1000.0, "2025-01-01", "engineering", "fr", "paris", "active"),
      (2, "Bob", "bob@x.com", 1500.0, "2025-01-02", "finance", "fr", "lyon", "active")
    ).toDF("id", "full_name", "email", "salary", "join_date", "department", "country", "city", "status")

    val cfg = PipelineConfig.load("application.conf")
    val star = StarSchemaBuilder.build(spark, df, cfg.warehouse)

    assertEquals(star.dimDepartment.count(), 2)
    assertEquals(star.dimEmployee.count(), 2)
    assertEquals(star.dimDate.count(), 2)
    assertEquals(star.factEmployeeMetrics.count(), 2)
  }
}
