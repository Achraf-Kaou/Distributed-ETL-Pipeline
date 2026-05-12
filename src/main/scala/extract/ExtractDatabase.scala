package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties

/**
 * ExtractDatabase — JDBC-based relational database ingestion into Spark DataFrames.
 *
 * Supports three database engines commonly used in data engineering workflows:
 *   - **PostgreSQL** — the primary source database in this pipeline (Docker: port 5432).
 *   - **MySQL** — secondary source demonstrating multi-engine JDBC support (Docker: port 3306).
 *   - **SQLite** — file-based database; useful for lightweight local testing without Docker.
 *
 * Two read modes are provided:
 *
 *   1. [[read]] — single-partition sequential read. Suitable for small-to-medium tables
 *      (< a few million rows) or when the source database cannot handle parallel connections.
 *      All data flows through a single Spark task via a single JDBC connection.
 *
 *   2. [[readPartitioned]] — parallel multi-partition read using Spark's built-in JDBC
 *      partitioning. Splits the table on a numeric column into `numPartitions` ranges,
 *      each fetched by a separate Spark task with its own JDBC connection. This is the
 *      recommended approach for large tables and is configured via `partition-column`,
 *      `lower-bound`, `upper-bound`, and `num-partitions` in `application.conf`.
 *
 * JDBC connection parameters are encapsulated in sealed [[DbConfig]] case classes
 * (one per database type) to prevent mixing incompatible connection parameters.
 *
 * Security note:
 * Credentials are read from `application.conf`. In production, replace plain-text
 * passwords with environment variable substitution (`${?DB_PASSWORD}` in HOCON) or
 * a secrets management solution (Vault, AWS Secrets Manager, etc.).
 */
object ExtractDatabase {

  // ─────────────────────────────────────────────────────────────────
  // Database configuration sealed hierarchy
  // ─────────────────────────────────────────────────────────────────

  /**
   * Common interface for all database connection configurations.
   * Sealed so the compiler can exhaustively check match expressions.
   */
  sealed trait DbConfig {
    /** Full JDBC URL string, e.g. `"jdbc:postgresql://localhost:5432/etl_db"`. */
    def jdbcUrl: String
    /** Fully-qualified JDBC driver class name, e.g. `"org.postgresql.Driver"`. */
    def driver: String
    /** Java Properties containing `user`, `password`, and `driver` for Spark JDBC. */
    def props: Properties
  }

  /**
   * PostgreSQL connection configuration.
   *
   * The JDBC URL uses the standard `jdbc:postgresql://` prefix.
   * SSL is enabled by default in the PostgreSQL driver; no extra options needed
   * for local/Docker connections where SSL is not configured.
   *
   * @param host     Database host. Example: `"localhost"`.
   * @param port     Database port. Default: `5432`.
   * @param database Database (catalog) name. Example: `"etl_db"`.
   * @param user     PostgreSQL username.
   * @param password PostgreSQL password.
   */
  case class PostgresConfig(
    host:     String,
    port:     Int    = 5432,
    database: String,
    user:     String,
    password: String
  ) extends DbConfig {
    val driver  = "org.postgresql.Driver"
    val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"
    val props   = buildProps(user, password, driver)
  }

  /**
   * MySQL 8 connection configuration.
   *
   * Extra URL parameters:
   *   - `useSSL=false`               — disable SSL for local/Docker connections.
   *   - `serverTimezone=UTC`         — avoid timezone-related parsing issues.
   *   - `allowPublicKeyRetrieval=true` — required for MySQL 8 with native auth plugin.
   *
   * @param host     Database host. Example: `"localhost"`.
   * @param port     Database port. Default: `3306`.
   * @param database MySQL schema/database name.
   * @param user     MySQL username.
   * @param password MySQL password.
   */
  case class MySQLConfig(
    host:     String,
    port:     Int    = 3306,
    database: String,
    user:     String,
    password: String
  ) extends DbConfig {
    val driver  = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = s"jdbc:mysql://$host:$port/$database?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    val props   = buildProps(user, password, driver)
  }

  /**
   * SQLite connection configuration — file-based, no host, port, or credentials.
   *
   * SQLite is single-writer by design. Partitioned reads are technically possible
   * but open multiple read connections simultaneously; use [[read]] (single partition)
   * with SQLite to avoid file locking issues.
   *
   * @param filePath Absolute or relative path to the `.db` file.
   *                 Example: `"docker/sqlite/data/etl.db"`.
   */
  case class SQLiteConfig(filePath: String) extends DbConfig {
    val driver  = "org.sqlite.JDBC"
    val jdbcUrl = s"jdbc:sqlite:$filePath"
    val props   = {
      val p = new Properties()
      p.setProperty("driver", driver)
      p
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Read methods
  // ─────────────────────────────────────────────────────────────────

  /**
   * Read a full table or SQL query result from a relational database via JDBC.
   *
   * Uses a single Spark partition (one JDBC connection, one task). The result is
   * a lazy DataFrame — no data is fetched until a Spark Action is called.
   *
   * The `fetchsize` option controls how many rows are fetched per JDBC round-trip
   * to the database. The default of 1000 balances memory pressure on the driver
   * against network round-trip overhead. Increase for large wide tables.
   *
   * The `tableOrQuery` parameter accepts either:
   *   - A plain table name: `"employees"`
   *   - A parenthesised subquery with alias: `"(SELECT * FROM employees WHERE active=1) t"`
   *     The alias (`t`) is required by the JDBC spec when using subqueries.
   *
   * @param spark        Active [[SparkSession]].
   * @param config       Typed [[DbConfig]] instance (Postgres / MySQL / SQLite).
   * @param tableOrQuery Table name or parenthesised SQL subquery.
   * @return             Lazy DataFrame with inferred schema.
   */
  def read(
    spark:        SparkSession,
    config:       DbConfig,
    tableOrQuery: String
  ): DataFrame = {

    val dbType = config.getClass.getSimpleName.replace("Config", "")
    println(s"📦 [$dbType] Connecting via JDBC: ${config.jdbcUrl}")
    println(s"📖 Reading: $tableOrQuery")

    try {
      val df = spark.read
        .format("jdbc")
        .option("url",       config.jdbcUrl)
        .option("dbtable",   tableOrQuery)
        .option("driver",    config.driver)
        .options(propsToMap(config.props))
        // fetchsize: rows fetched per JDBC round-trip. 1000 is a safe default;
        // increase for large tables to reduce network overhead.
        .option("fetchsize", "1000")
        .load()

      println(s"✅ [$dbType] Loaded ${df.count()} rows | ${df.columns.length} columns")
      df

    } catch {
      case e: Exception =>
        println(s"❌ [$dbType] JDBC read failed: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Read a database table in parallel using Spark's JDBC partitioning.
   *
   * Spark splits the numeric range [`lowerBound`, `upperBound`] on `partitionColumn`
   * into `numPartitions` equal-width buckets. Each bucket is fetched by a separate
   * Spark task with its own JDBC connection, enabling true parallel ingestion.
   *
   * Important constraints:
   *   - `partitionColumn` must be a numeric column (INT, BIGINT, etc.).
   *   - `lowerBound` / `upperBound` do not filter rows — they only define the split
   *     boundaries. Rows outside this range are still included in the first/last partition.
   *   - Set `numPartitions` to match the database's connection pool capacity to avoid
   *     overwhelming the source system. A value of 4–8 is reasonable for most setups.
   *   - Avoid using this on SQLite (single-writer file locking).
   *
   * @param spark           Active [[SparkSession]].
   * @param config          Typed [[DbConfig]] instance.
   * @param table           Table name (plain, not a subquery — Spark wraps it internally).
   * @param partitionColumn Numeric column to partition on. Example: `"id"`.
   * @param lowerBound      Minimum value of `partitionColumn` (approximate is fine).
   * @param upperBound      Maximum value of `partitionColumn`.
   * @param numPartitions   Number of parallel Spark tasks / JDBC connections. Default: 4.
   * @return                DataFrame composed of all partitions unioned together.
   */
  def readPartitioned(
    spark:           SparkSession,
    config:          DbConfig,
    table:           String,
    partitionColumn: String,
    lowerBound:      Long,
    upperBound:      Long,
    numPartitions:   Int = 4
  ): DataFrame = {

    val dbType = config.getClass.getSimpleName.replace("Config", "")
    println(s"📦 [$dbType] Partitioned read on '$table' via column '$partitionColumn'")
    println(s"   Range: $lowerBound → $upperBound | $numPartitions partitions")

    try {
      val df = spark.read
        .format("jdbc")
        .option("url",             config.jdbcUrl)
        .option("dbtable",         table)
        .option("driver",          config.driver)
        .options(propsToMap(config.props))
        .option("partitionColumn", partitionColumn)
        .option("lowerBound",      lowerBound)
        .option("upperBound",      upperBound)
        .option("numPartitions",   numPartitions)
        .option("fetchsize",       "1000")
        .load()

      println(s"✅ [$dbType] Loaded ${df.count()} rows across $numPartitions partitions")
      df

    } catch {
      case e: Exception =>
        println(s"❌ [$dbType] Partitioned read failed: ${e.getMessage}")
        throw e
    }
  }

  // ─────────────────────────────────────────────────────────────────
  // Development / debugging utility
  // ─────────────────────────────────────────────────────────────────

  /**
   * Print schema, up to 50 preview rows, and total count for a database-sourced DataFrame.
   * For interactive development and debugging only.
   *
   * @param df          DataFrame returned by [[read]] or [[readPartitioned]].
   * @param sourceName  Display label shown in the output header.
   */
  def printContent(df: DataFrame, sourceName: String = "DB Source"): Unit = {
    println(s"\n=== 🗄️  $sourceName ===")
    df.printSchema()
    println("\nPreview (up to 50 rows):")
    df.show(50, truncate = false)
    println(s"Total rows: ${df.count()}")
    println("-" * 90)
  }

  // ─────────────────────────────────────────────────────────────────
  // Internal helpers
  // ─────────────────────────────────────────────────────────────────

  /**
   * Build a standard Java [[Properties]] object with `user`, `password`, and `driver`.
   * Passed to Spark's JDBC reader as connection properties.
   */
  private def buildProps(user: String, password: String, driver: String): Properties = {
    val p = new Properties()
    p.setProperty("user",     user)
    p.setProperty("password", password)
    p.setProperty("driver",   driver)
    p
  }

  /**
   * Convert [[java.util.Properties]] to `Map[String, String]` for Spark's `.options()` API.
   */
  private def propsToMap(props: Properties): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    props.stringPropertyNames().asScala.map(k => k -> props.getProperty(k)).toMap
  }
}