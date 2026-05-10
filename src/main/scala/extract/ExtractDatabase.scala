package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties


object ExtractDatabase {

  sealed trait DbConfig {
    def jdbcUrl: String
    def driver: String
    def props: Properties
  }

  /**
   * PostgreSQL configuration.
   *
   * @param host     e.g. "localhost"
   * @param port     default 5432
   * @param database database name
   * @param user     DB username
   * @param password DB password
   */
  case class PostgresConfig(
    host: String,
    port: Int = 5432,
    database: String,
    user: String,
    password: String
  ) extends DbConfig {
    val driver  = "org.postgresql.Driver"
    val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"
    val props   = buildProps(user, password, driver)
  }

  /**
   * MySQL 8 configuration.
   *
   * @param host     e.g. "localhost"
   * @param port     default 3306
   * @param database database / schema name
   * @param user     DB username
   * @param password DB password
   */
  case class MySQLConfig(
    host: String,
    port: Int = 3306,
    database: String,
    user: String,
    password: String
  ) extends DbConfig {
    val driver  = "com.mysql.cj.jdbc.Driver"
    val jdbcUrl = s"jdbc:mysql://$host:$port/$database?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
    val props   = buildProps(user, password, driver)
  }

  /**
   * SQLite configuration — file-based, no host/port/credentials.
   *
   * @param filePath absolute or relative path to the .db file
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

  /**
   * Read a full table (or any SQL query) from a relational database.
   *
   * @param spark     active SparkSession
   * @param config    typed DbConfig (Postgres / MySQL / SQLite)
   * @param tableOrQuery  either a table name ("employees") or a
   *                      parenthesised subquery ("(SELECT * FROM emp WHERE active=1) t")
   * @return          DataFrame with inferred schema
   */
  def read(
    spark: SparkSession,
    config: DbConfig,
    tableOrQuery: String
  ): DataFrame = {

    val dbType = config.getClass.getSimpleName.replace("Config", "")
    println(s"📦 [$dbType] Connecting via JDBC: ${config.jdbcUrl}")
    println(s"📖 Reading: $tableOrQuery")

    try {
      val df = spark.read
        .format("jdbc")
        .option("url",    config.jdbcUrl)
        .option("dbtable", tableOrQuery)
        .option("driver", config.driver)
        .options(propsToMap(config.props))
        // fetchsize: how many rows are pulled per JDBC round-trip.
        // 1000 is a good default; tune up for large result sets.
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
   *
   * @param partitionColumn  numeric column to split on (e.g. "id")
   * @param lowerBound       minimum value of partitionColumn
   * @param upperBound       maximum value of partitionColumn
   * @param numPartitions    number of parallel Spark tasks (e.g. 4)
   */
  def readPartitioned(
    spark: SparkSession,
    config: DbConfig,
    table: String,
    partitionColumn: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int = 4
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

  def printContent(df: DataFrame, sourceName: String = "DB Source"): Unit = {
    println(s"\n=== 🗄️  $sourceName ===")
    df.printSchema()
    println("\nPreview (up to 50 rows):")
    df.show(50, truncate = false)
    println(s"Total rows: ${df.count()}")
    println("-" * 90)
  }

  private def buildProps(user: String, password: String, driver: String): Properties = {
    val p = new Properties()
    p.setProperty("user",     user)
    p.setProperty("password", password)
    p.setProperty("driver",   driver)
    p
  }

  /** Convert java.util.Properties → Map[String, String] for Spark options */
  private def propsToMap(props: Properties): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    props.stringPropertyNames().asScala.map(k => k -> props.getProperty(k)).toMap
  }
}