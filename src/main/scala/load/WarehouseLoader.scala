package load

import config.PipelineConfig.{UpsertConfig, WarehouseConfig, WarehouseTargetConfig}
import extract.ExtractDatabase
import java.io.File
import org.apache.spark.sql.DataFrame
import java.sql.{Connection, DriverManager}
import scala.util.Using

/**
 * WarehouseLoader — JDBC upsert loader for the star-schema warehouse target.
 *
 * This object is responsible for the final "load" step: persisting the four
 * star-schema DataFrames (three dimensions + one fact table) produced by
 * [[StarSchemaBuilder]] into a relational database via JDBC upsert semantics.
 *
 * == Upsert Strategy (Staging Table Pattern) ==
 *
 * A direct `INSERT` would fail on duplicate business keys; a `DELETE + INSERT`
 * would lose data if the pipeline crashes mid-flight. The staging-table pattern
 * used here is the standard production approach:
 *
 * {{{
 *   1. WRITE new data → staging table (`stg_<target>`) using Spark JDBC overwrite.
 *   2. CREATE target table IF NOT EXISTS (schema derived from DataFrame schema).
 *   3. UPSERT from staging → target using database-native upsert syntax:
 *        - PostgreSQL / SQLite: INSERT ... ON CONFLICT (...) DO UPDATE SET ...
 *        - MySQL:               INSERT ... ON DUPLICATE KEY UPDATE ...
 * }}}
 *
 * This pattern is atomic per table (the upsert SQL runs inside a single JDBC
 * statement), idempotent (re-running produces the same result), and does not
 * require the target table to be empty beforehand.
 *
 * == Supported Warehouse Targets ==
 *
 * | DB Type    | Port | Upsert Syntax                       |
 * |------------|------|-------------------------------------|
 * | PostgreSQL | 5433 | ON CONFLICT (...) DO UPDATE SET ... |
 * | MySQL      | 3306 | ON DUPLICATE KEY UPDATE ...         |
 * | SQLite     | file | ON CONFLICT (...) DO UPDATE SET ... |
 *
 * == SQL Injection Prevention ==
 *
 * All table and column identifiers are passed through [[quotedIdentifier]] before
 * being interpolated into SQL strings. This method validates that the identifier
 * matches `^[A-Za-z_][A-Za-z0-9_]*$` and applies the appropriate quoting
 * (`"..."` for PostgreSQL/SQLite, backticks for MySQL). This prevents SQL injection
 * from malformed config values.
 *
 * == Enabling the Loader ==
 *
 * The loader only runs when all three flags are true in `application.conf`:
 * {{{
 *   etl.warehouse.enabled         = true
 *   etl.load.warehouse-target.enabled = true
 *   etl.load.upsert.enabled       = true
 * }}}
 */
object WarehouseLoader {

  /**
   * Load all star-schema tables into the configured warehouse target database.
   *
   * Iterates over the dimension tables declared in `warehouseConfig.dimensions`,
   * looks up the corresponding DataFrame from the [[StarSchemaBuilder.StarSchemaResult]],
   * and upserts each one. Then upserts the fact table.
   *
   * Tables not present in `warehouseConfig.dimensions` (e.g. a dimension added to
   * [[StarSchemaResult]] but not declared in config) are silently skipped.
   *
   * @param star            Star-schema DataFrames produced by [[StarSchemaBuilder.build]].
   * @param warehouseConfig Warehouse schema configuration (dimension names, keys).
   * @param target          JDBC connection details for the warehouse database.
   * @param upsertConfig    Upsert behaviour settings (staging prefix, batch size).
   */
  def loadStarSchema(
    star:            StarSchemaBuilder.StarSchemaResult,
    warehouseConfig: WarehouseConfig,
    target:          WarehouseTargetConfig,
    upsertConfig:    UpsertConfig
  ): Unit = {
    // Guard: all three flags must be enabled; otherwise this is a no-op.
    if (!target.enabled || !upsertConfig.enabled || !warehouseConfig.enabled) return

    val db             = target.dbType.trim.toLowerCase
    val (url, driver, props) = jdbcOptions(target)

    // Map dimension table name → its DataFrame for config-driven lookup.
    val dimensionsByName = Map(
      "dim_department" -> star.dimDepartment,
      "dim_employee"   -> star.dimEmployee,
      "dim_date"       -> star.dimDate
    )

    // Load each declared dimension in config order.
    warehouseConfig.dimensions.foreach { t =>
      dimensionsByName.get(t.name).foreach { df =>
        upsertDataFrame(df, t.name, t.businessKeys, url, driver, props, db, upsertConfig.stagingPrefix, upsertConfig.batchSize)
      }
    }

    // Load the fact table last (dimensions must exist first for FK integrity).
    upsertDataFrame(
      star.factEmployeeMetrics,
      warehouseConfig.fact.name,
      warehouseConfig.fact.businessKeys,
      url, driver, props, db,
      upsertConfig.stagingPrefix,
      upsertConfig.batchSize
    )
  }

  // ─────────────────────────────────────────────────────────────────
  // Core upsert logic
  // ─────────────────────────────────────────────────────────────────

  /**
   * Upsert a single DataFrame into a target table using the staging pattern.
   *
   * Steps:
   *   1. Write the DataFrame to a staging table (overwrite mode) via Spark JDBC.
   *   2. Open a plain JDBC connection to the warehouse.
   *   3. Create the target table if it does not exist (schema from DataFrame).
   *   4. Execute the database-native UPSERT from staging → target.
   *
   * The staging table is named `<stagingPrefix><targetTable>` (default: `stg_<table>`).
   * It is overwritten on every pipeline run — it is transient scratch space only.
   *
   * @param df             DataFrame to persist (dimension or fact).
   * @param targetTable    Name of the final target table, e.g. `"dim_employee"`.
   * @param businessKeys   Columns that form the unique constraint for conflict detection.
   * @param jdbcUrl        Full JDBC connection URL.
   * @param driver         JDBC driver class name.
   * @param props          Connection properties (user, password).
   * @param dbType         Database type string: `"postgres"`, `"mysql"`, or `"sqlite"`.
   * @param stagingPrefix  Prefix for the staging table name. Default: `"stg_"`.
   * @param batchSize      Rows per JDBC batch insert. Default: 500.
   */
  private def upsertDataFrame(
    df:            DataFrame,
    targetTable:   String,
    businessKeys:  Seq[String],
    jdbcUrl:       String,
    driver:        String,
    props:         java.util.Properties,
    dbType:        String,
    stagingPrefix: String,
    batchSize:     Int = 500
  ): Unit = {
    if (df.isEmpty) return

    val safeTarget       = quotedIdentifier(targetTable, dbType)
    val stagingTableName = s"$stagingPrefix$targetTable"
    val safeStaging      = quotedIdentifier(stagingTableName, dbType)

    // Step 1 — write to staging via Spark JDBC (parallel, batched).
    df.write.mode("overwrite").format("jdbc")
      .option("url",       jdbcUrl)
      .option("dbtable",   stagingTableName)
      .option("driver",    driver)
      .option("batchsize", batchSize)
      .options(propsToMap(props))
      .save()

    // Steps 2–4 — DDL + UPSERT via a plain JDBC connection.
    Using.resource(DriverManager.getConnection(jdbcUrl, props)) { conn =>
      ensureTable(conn, safeTarget, df, dbType, businessKeys)
      val sql = buildUpsertSql(safeTarget, safeStaging, df.columns.toSeq, businessKeys, dbType)
      executeSql(conn, sql)
    }
  }

  /**
   * Create the target table if it does not exist.
   *
   * The schema is derived from the DataFrame's Spark schema using a simple
   * type mapping (see below). A UNIQUE constraint is added on `businessKeys`
   * to enable conflict detection in the upsert step.
   *
   * Type mapping:
   * | Spark type | SQL type         |
   * |------------|------------------|
   * | integer    | INTEGER          |
   * | long       | BIGINT           |
   * | double     | DOUBLE PRECISION |
   * | date       | DATE             |
   * | (other)    | TEXT             |
   */
  private def ensureTable(
    conn:         Connection,
    table:        String,
    df:           DataFrame,
    dbType:       String,
    businessKeys: Seq[String]
  ): Unit = {
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
    val unique     = if (uniqueKeys.nonEmpty) s", UNIQUE (${uniqueKeys.mkString(",")})" else ""
    val createSql  = s"CREATE TABLE IF NOT EXISTS $table ($cols$unique)"
    executeSql(conn, createSql)
  }

  /** Execute a SQL statement using a short-lived Statement, auto-closed via [[Using]]. */
  private def executeSql(conn: Connection, sql: String): Unit =
    Using.resource(conn.createStatement())(_.execute(sql))

  /**
   * Build the database-native UPSERT SQL string.
   *
   * Logic:
   *   - If there are no business keys, emits a plain INSERT (no conflict handling).
   *   - If there are no non-key columns to update, emits INSERT ... DO NOTHING (or IGNORE).
   *   - Otherwise, emits INSERT ... ON CONFLICT (...) DO UPDATE SET / ON DUPLICATE KEY UPDATE.
   *
   * @param targetTable  Quoted target table identifier.
   * @param stagingTable Quoted staging table identifier.
   * @param columns      All column names in the DataFrame.
   * @param businessKeys Columns forming the unique constraint.
   * @param dbType       `"postgres"` | `"postgresql"` | `"mysql"` | `"sqlite"`.
   * @return             Complete UPSERT SQL string ready to execute.
   */
  private def buildUpsertSql(
    targetTable:  String,
    stagingTable: String,
    columns:      Seq[String],
    businessKeys: Seq[String],
    dbType:       String
  ): String = {
    val safeColumns      = columns.map(c => quotedIdentifier(c, dbType))
    val safeBusinessKeys = businessKeys.map(k => quotedIdentifier(k, dbType))
    val colList          = safeColumns.mkString(",")
    val nonKeyCols       = columns.filterNot(c => businessKeys.contains(c))
    val safeNonKeyCols   = nonKeyCols.map(c => quotedIdentifier(c, dbType))

    // SQLite requires an explicit WHERE clause in the SELECT from staging.
    val selectSource = dbType match {
      case "sqlite" => s"SELECT $colList FROM $stagingTable WHERE 1=1"
      case _        => s"SELECT $colList FROM $stagingTable"
    }
    val plainInsert = s"INSERT INTO $targetTable ($colList) $selectSource"

    if (safeBusinessKeys.isEmpty) return plainInsert

    // Keys-only table: insert or ignore duplicate keys.
    if (safeNonKeyCols.isEmpty) {
      return dbType match {
        case "postgres" | "postgresql" | "sqlite" =>
          plainInsert + s" ON CONFLICT (${safeBusinessKeys.mkString(",")}) DO NOTHING"
        case "mysql" =>
          s"INSERT IGNORE INTO $targetTable ($colList) SELECT $colList FROM $stagingTable"
        case _ => plainInsert
      }
    }

    // Full upsert: update non-key columns on conflict.
    dbType match {
      case "postgres" | "postgresql" | "sqlite" =>
        val updates = safeNonKeyCols.map(c => s"$c=excluded.$c").mkString(",")
        plainInsert + " " + s"ON CONFLICT (${safeBusinessKeys.mkString(",")}) DO UPDATE SET $updates"

      case "mysql" =>
        val updates = safeNonKeyCols.map(c => s"$c=VALUES($c)").mkString(",")
        plainInsert + " " + s"ON DUPLICATE KEY UPDATE $updates"

      case other =>
        throw new IllegalArgumentException(s"Unsupported upsert db-type: $other")
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Connection helpers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Resolve JDBC connection triple (url, driver, props) from a [[WarehouseTargetConfig]].
   *
   * Reuses the [[ExtractDatabase]] config classes to avoid duplicating JDBC URL
   * construction logic. For SQLite, ensures the parent directory of the `.db`
   * file exists before Spark tries to write to it.
   */
  private def jdbcOptions(
    target: WarehouseTargetConfig
  ): (String, String, java.util.Properties) = {
    val typedCfg = target.dbType.trim.toLowerCase match {
      case "postgres" | "postgresql" =>
        ExtractDatabase.PostgresConfig(
          host     = target.host,
          port     = if (target.port > 0) target.port else 5432,
          database = target.database,
          user     = target.user,
          password = target.password
        )
      case "mysql" =>
        ExtractDatabase.MySQLConfig(
          host     = target.host,
          port     = if (target.port > 0) target.port else 3306,
          database = target.database,
          user     = target.user,
          password = target.password
        )
      case "sqlite" =>
        // Create parent directory for the SQLite file; Spark's JDBC fails silently
        // if the path's parent directory does not yet exist.
        val parent = new File(target.sqliteFilePath).getParentFile
        if (parent != null && !parent.exists()) parent.mkdirs()
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

  /**
   * Quote and validate a SQL identifier to prevent injection and syntax errors.
   *
   * Allowed characters: letters, digits, underscore; must not start with a digit.
   * Quoting style differs by database:
   *   - MySQL: backtick  → `` `identifier` ``
   *   - All others: double-quote → `"identifier"`
   *
   * @throws IllegalArgumentException if the identifier contains invalid characters.
   */
  private def quotedIdentifier(identifier: String, dbType: String): String = {
    val allowedPattern = "^[A-Za-z_][A-Za-z0-9_]*$"
    require(
      identifier.matches(allowedPattern),
      s"Unsafe SQL identifier '$identifier'. Only letters, digits and underscore are allowed, and it must not start with a digit."
    )
    dbType match {
      case "mysql" => s"`$identifier`"
      case _       => "\"" + identifier + "\""
    }
  }
}
