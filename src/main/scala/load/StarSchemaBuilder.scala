package load

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import config.PipelineConfig.WarehouseConfig

object StarSchemaBuilder {

  case class StarSchemaResult(
    dimDepartment: DataFrame,
    dimEmployee: DataFrame,
    dimDate: DataFrame,
    factEmployeeMetrics: DataFrame
  )

  def build(spark: SparkSession, df: DataFrame, warehouseConfig: WarehouseConfig): StarSchemaResult = {
    import spark.implicits._

    val departmentCol = choose(df.columns, Seq("department", "dim_dept_name"))
    val employeeIdCol = choose(df.columns, Seq("email", "id"))
    val joinDateCol = choose(df.columns, Seq("join_date"))

    if (departmentCol.isEmpty) println("[WARN] Star schema: missing department column, dim_department will be empty")
    if (employeeIdCol.isEmpty) println("[WARN] Star schema: missing employee business key column, dim_employee will be empty")
    if (joinDateCol.isEmpty) println("[WARN] Star schema: missing join_date column, dim_date will be empty")

    val dimDepartment = if (departmentCol.nonEmpty) {
      df.select(col(departmentCol).as("department_name"))
        .where(col("department_name").isNotNull)
        .dropDuplicates()
        .withColumn("department_sk", dense_rank().over(Window.orderBy(col("department_name"))))
    } else spark.emptyDataFrame

    val dimEmployee = if (employeeIdCol.nonEmpty) {
      val selectedCols = Seq("full_name", "email", "age", "status", "country", "city").filter(df.columns.contains).map(col)
      df.select((col(employeeIdCol).as("employee_bk") +: selectedCols): _*)
        .where(col("employee_bk").isNotNull)
        .dropDuplicates("employee_bk")
        .withColumn("employee_sk", dense_rank().over(Window.orderBy(col("employee_bk"))))
    } else spark.emptyDataFrame

    val dimDate = if (joinDateCol.nonEmpty) {
      df.select(to_date(col(joinDateCol)).as("date_value"))
        .where(col("date_value").isNotNull)
        .dropDuplicates()
        .withColumn("date_sk", date_format(col("date_value"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("date_value")))
        .withColumn("month", month(col("date_value")))
        .withColumn("day", dayofmonth(col("date_value")))
    } else spark.emptyDataFrame

    val base = df
      .withColumn("employee_bk", if (employeeIdCol.nonEmpty) col(employeeIdCol) else lit(null))
      .withColumn("department_name", if (departmentCol.nonEmpty) col(departmentCol) else lit(null))
      .withColumn("date_value", if (joinDateCol.nonEmpty) to_date(col(joinDateCol)) else lit(null).cast("date"))
      .withColumn("salary_value", if (df.columns.contains("salary")) col("salary").cast("double") else lit(0.0))

    val factEmployeeMetrics = base
      .join(dimEmployee.select("employee_sk", "employee_bk"), Seq("employee_bk"), "left")
      .join(dimDepartment.select("department_sk", "department_name"), Seq("department_name"), "left")
      .join(dimDate.select("date_sk", "date_value"), Seq("date_value"), "left")
      .select(
        col("employee_sk"),
        col("department_sk"),
        col("date_sk"),
        col("salary_value").as("salary"),
        lit(1L).as("employee_count")
      )
      .where(col("employee_sk").isNotNull && col("department_sk").isNotNull && col("date_sk").isNotNull)

    StarSchemaResult(dimDepartment, dimEmployee, dimDate, factEmployeeMetrics)
  }

  private def choose(columns: Seq[String], candidates: Seq[String]): String =
    candidates.find(columns.contains).getOrElse("")
}
