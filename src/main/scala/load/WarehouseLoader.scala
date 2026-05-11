package load

import config.PipelineConfig.{UpsertConfig, WarehouseConfig, WarehouseTargetConfig}
import extract.ExtractDatabase
import org.apache.spark.sql.DataFrame
import java.sql.{Connection, DriverManager}
import scala.util.Using

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
    val safeTarget = quotedIdentifier(targetTable, dbType)
    val stagingTableName = s"$stagingPrefix$targetTable"
    val safeStaging = quotedIdentifier(stagingTableName, dbType)
    df.write.mode("overwrite").format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", stagingTableName)
      .option("driver", driver)
      .option("batchsize", batchSize)
      .options(propsToMap(props))
      .save()

    Using.resource(DriverManager.getConnection(jdbcUrl, props)) { conn =>
      ensureTable(conn, safeTarget, df, dbType, businessKeys)
      val sql = buildUpsertSql(safeTarget, safeStaging, df.columns.toSeq, businessKeys, dbType)
      executeSql(conn, sql)
    }
  }

  private def ensureTable(conn: Connection, table: String, df: DataFrame, dbType: String, businessKeys: Seq[String]): Unit = {
    val cols = df.schema.fields.map { f =>
      val safeCol = quotedIdentifier(f.name, dbType)
      val t = f.dataType.typeName.toLowerCase match {
        case "integer" => "INTEGER"
        case "long"    => "BIGINT"
        case "double"  => "DOUBLE PRECISION"
        case "date"    => "DATE"
        case _         => "TEXT"
      }
      s"$safeCol $t"
    }.mkString(", ")
    val uniqueKeys = businessKeys.map(k => quotedIdentifier(k, dbType))
    val unique = if (uniqueKeys.nonEmpty) s", UNIQUE (${uniqueKeys.mkString(",")})" else ""
    val createSql = s"CREATE TABLE IF NOT EXISTS $table ($cols$unique)"
    executeSql(conn, createSql)
  }

  private def executeSql(conn: Connection, sql: String): Unit =
    Using.resource(conn.createStatement())(_.execute(sql))

  private def buildUpsertSql(
    targetTable: String,
    stagingTable: String,
    columns: Seq[String],
    businessKeys: Seq[String],
    dbType: String
  ): String = {
    val safeColumns = columns.map(c => quotedIdentifier(c, dbType))
    val safeBusinessKeys = businessKeys.map(k => quotedIdentifier(k, dbType))
    val colList = safeColumns.mkString(",")
    val nonKeyCols = columns.filterNot(c => businessKeys.contains(c))
    val safeNonKeyCols = nonKeyCols.map(c => quotedIdentifier(c, dbType))
    val plainInsert = s"INSERT INTO $targetTable ($colList) SELECT $colList FROM $stagingTable"

    if (safeBusinessKeys.isEmpty) return plainInsert

    dbType match {
      case "postgres" | "postgresql" | "sqlite" =>
        val updates = safeNonKeyCols.map(c => s"$c=excluded.$c").mkString(",")
        plainInsert + " " +
          s"ON CONFLICT (${safeBusinessKeys.mkString(",")}) DO UPDATE SET $updates"
      case "mysql" =>
        val updates = safeNonKeyCols.map(c => s"$c=VALUES($c)").mkString(",")
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

  private def quotedIdentifier(identifier: String, dbType: String): String = {
    val allowedPattern = "^[A-Za-z_][A-Za-z0-9_]*$"
    require(
      identifier.matches(allowedPattern),
      s"Unsafe SQL identifier '$identifier'. Only letters, digits and underscore are allowed, and it must not start with a digit."
    )
    dbType match {
      case "mysql" => s"`$identifier`"
      case _       => s""""$identifier""""
    }
  }
}
