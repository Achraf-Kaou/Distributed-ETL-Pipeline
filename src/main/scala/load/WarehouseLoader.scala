package load

import config.PipelineConfig.{UpsertConfig, WarehouseConfig, WarehouseTargetConfig}
import extract.ExtractDatabase
import org.apache.spark.sql.DataFrame
import java.sql.{Connection, DriverManager}

object WarehouseLoader {

  def loadStarSchema(
    star: StarSchemaBuilder.StarSchemaResult,
    warehouseConfig: WarehouseConfig,
    target: WarehouseTargetConfig,
    upsertConfig: UpsertConfig
  ): Unit = {
    if (!target.enabled || !upsertConfig.enabled || !warehouseConfig.enabled) return

    val db = target.dbType.trim.toLowerCase
    val (url, driver, props) = jdbcOptions(target)

    val dimensionsByName = Map(
      "dim_department" -> star.dimDepartment,
      "dim_employee" -> star.dimEmployee,
      "dim_date" -> star.dimDate
    )

    warehouseConfig.dimensions.foreach { t =>
      dimensionsByName.get(t.name).foreach { df =>
        upsertDataFrame(df, t.name, t.businessKeys, url, driver, props, db, upsertConfig.stagingPrefix, upsertConfig.batchSize)
      }
    }

    upsertDataFrame(
      star.factEmployeeMetrics,
      warehouseConfig.fact.name,
      warehouseConfig.fact.businessKeys,
      url,
      driver,
      props,
      db,
      upsertConfig.stagingPrefix,
      upsertConfig.batchSize
    )
  }

  private def upsertDataFrame(
    df: DataFrame,
    targetTable: String,
    businessKeys: Seq[String],
    jdbcUrl: String,
    driver: String,
    props: java.util.Properties,
    dbType: String,
    stagingPrefix: String,
    batchSize: Int = 500
  ): Unit = {
    if (df.isEmpty) return
    val stagingTable = s"$stagingPrefix$targetTable"
    df.write.mode("overwrite").format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", stagingTable)
      .option("driver", driver)
      .option("batchsize", batchSize)
      .options(propsToMap(props))
      .save()

    val conn = DriverManager.getConnection(jdbcUrl, props)
    try {
      ensureTable(conn, targetTable, df, dbType, businessKeys)
      val sql = buildUpsertSql(targetTable, stagingTable, df.columns.toSeq, businessKeys, dbType)
      val st = conn.createStatement()
      try st.execute(sql) finally st.close()
    } finally conn.close()
  }

  private def ensureTable(conn: Connection, table: String, df: DataFrame, dbType: String, businessKeys: Seq[String]): Unit = {
    val cols = df.schema.fields.map { f =>
      val t = f.dataType.typeName.toLowerCase match {
        case "integer" => "INTEGER"
        case "long"    => "BIGINT"
        case "double"  => "DOUBLE PRECISION"
        case "date"    => "DATE"
        case _         => "TEXT"
      }
      s"${f.name} $t"
    }.mkString(", ")
    val unique = if (businessKeys.nonEmpty) s", UNIQUE (${businessKeys.mkString(",")})" else ""
    val createSql = dbType match {
      case "mysql" => s"CREATE TABLE IF NOT EXISTS $table ($cols$unique)"
      case _       => s"CREATE TABLE IF NOT EXISTS $table ($cols$unique)"
    }
    val st = conn.createStatement()
    try st.execute(createSql) finally st.close()
  }

  private def buildUpsertSql(
    targetTable: String,
    stagingTable: String,
    columns: Seq[String],
    businessKeys: Seq[String],
    dbType: String
  ): String = {
    val colList = columns.mkString(",")
    val selectList = columns.mkString(",")
    val nonKeyCols = columns.filterNot(c => businessKeys.contains(c))
    val plainInsert = s"INSERT INTO $targetTable ($colList) SELECT $selectList FROM $stagingTable"

    if (businessKeys.isEmpty) return plainInsert

    dbType match {
      case "postgres" | "postgresql" | "sqlite" =>
        val updates = nonKeyCols.map(c => s"$c=excluded.$c").mkString(",")
        plainInsert + " " +
          s"ON CONFLICT (${businessKeys.mkString(",")}) DO UPDATE SET $updates"
      case "mysql" =>
        val updates = nonKeyCols.map(c => s"$c=VALUES($c)").mkString(",")
        plainInsert + " " +
          s"ON DUPLICATE KEY UPDATE $updates"
      case other =>
        throw new IllegalArgumentException(s"Unsupported upsert db-type: $other")
    }
  }

  private def jdbcOptions(target: WarehouseTargetConfig): (String, String, java.util.Properties) = {
    val typedCfg = target.dbType.trim.toLowerCase match {
      case "postgres" | "postgresql" =>
        ExtractDatabase.PostgresConfig(
          host = target.host,
          port = if (target.port > 0) target.port else 5432,
          database = target.database,
          user = target.user,
          password = target.password
        )
      case "mysql" =>
        ExtractDatabase.MySQLConfig(
          host = target.host,
          port = if (target.port > 0) target.port else 3306,
          database = target.database,
          user = target.user,
          password = target.password
        )
      case "sqlite" =>
        ExtractDatabase.SQLiteConfig(target.sqliteFilePath)
      case other =>
        throw new IllegalArgumentException(s"Unsupported warehouse db-type: $other")
    }
    (typedCfg.jdbcUrl, typedCfg.driver, typedCfg.props)
  }

  private def propsToMap(props: java.util.Properties): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    props.stringPropertyNames().asScala.map(k => k -> props.getProperty(k)).toMap
  }
}
