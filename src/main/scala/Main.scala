import org.apache.spark.sql.SparkSession
import extract.ExtractFlatFiles
import extract.ExtractApi
import extract.ExtractDatabase
import transform.TransformClean
import transform.TransformClean.CleanConfig
import org.apache.spark.sql.types._
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Distributed ETL - Raw Data Processor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawFolderPath = "data/test"

    try {

      // Define cleaning rules
      val cleanConfig = CleanConfig(
        criticalColumns = Seq("id", "name", "email"),           // must have these
        fillValues = Map(
          "country" -> "Unknown",
          "age" -> "0",
          "salary" -> "0"
        ),
        stringColumns = Seq("name", "city", "country", "email"),
        castColumns = Map(
          "age" -> IntegerType,
          "salary" -> DoubleType,
          "join_date" -> DateType
        ),
        dropRowsWithNullsThreshold = 0.7,   // drop rows with >70% nulls
        verbose = true
      )

      extractFlatFiles(spark, rawFolderPath, cleanConfig)


      /* val dfApi = ExtractApi.read(
        spark = spark,
        url = "https://jsonplaceholder.typicode.com/users",
        rootField = ""
      )

      ExtractApi.printContent(dfApi, "API - Users") */

     /*  // ─── 1. PostgreSQL ─────────────────────────────────────────────────────
      // In production, load credentials from env vars or a secrets manager:
      //   sys.env.getOrElse("PG_USER", "etl_user")
      val pgConfig = ExtractDatabase.PostgresConfig(
        host     = "localhost",
        port     = 5432,
        database = "etl_db",
        user     = "etl_user",
        password = "etl_pass"
      )

      val pgUsers = ExtractDatabase.read(spark, pgConfig, "users")
      ExtractDatabase.printContent(pgUsers, "PostgreSQL → users")

      val pgProducts = ExtractDatabase.read(spark, pgConfig, "products")
      ExtractDatabase.printContent(pgProducts, "PostgreSQL → products")

      // Custom SQL query example — subquery must be aliased
      val pgExpensive = ExtractDatabase.read(
        spark, pgConfig,
        "(SELECT name, price, category FROM products WHERE price > 100.00) AS expensive"
      )
      ExtractDatabase.printContent(pgExpensive, "PostgreSQL → products WHERE price > 100")


      // ─── 2. MySQL 8 ────────────────────────────────────────────────────────
      val mysqlConfig = ExtractDatabase.MySQLConfig(
        host     = "localhost",
        port     = 3306,
        database = "etl_db",
        user     = "etl_user",
        password = "etl_pass"
      )

      val mysqlUsers = ExtractDatabase.read(spark, mysqlConfig, "users")
      ExtractDatabase.printContent(mysqlUsers, "MySQL → users")

      val mysqlProducts = ExtractDatabase.read(spark, mysqlConfig, "products")
      ExtractDatabase.printContent(mysqlProducts, "MySQL → products")

      // Partitioned read example — useful when users table grows large.
      // partitionColumn must be numeric; bounds should bracket the actual data.
      val mysqlPartitioned = ExtractDatabase.readPartitioned(
        spark         = spark,
        config        = mysqlConfig,
        table         = "users",
        partitionColumn = "id",
        lowerBound    = 1L,
        upperBound    = 1000L,
        numPartitions = 4
      )
      ExtractDatabase.printContent(mysqlPartitioned, "MySQL → users (partitioned read)")

      // ─── 3. SQLite ─────────────────────────────────────────────────────────
      // The path must match where docker-compose mounts the sqlite_data volume.
      // If running locally without Docker: point to any local .db file.
      val sqliteConfig = ExtractDatabase.SQLiteConfig(
        filePath = "./docker/sqlite/data/etl.db"
      )

      val sqliteUsers = ExtractDatabase.read(spark, sqliteConfig, "users")
      ExtractDatabase.printContent(sqliteUsers, "SQLite → users")

      val sqliteProducts = ExtractDatabase.read(spark, sqliteConfig, "products")
      ExtractDatabase.printContent(sqliteProducts, "SQLite → products")

      // ─── Cross-DB join example ─────────────────────────────────────────────
      // Real ETL scenario: merge users from two sources, deduplicate by email.
      println("\n=== 🔗 Cross-source merge: PostgreSQL + MySQL users ===")
      val merged = pgUsers
        .union(mysqlUsers)
        .dropDuplicates("email")   // deduplicate — same seed data in both DBs

      ExtractDatabase.printContent(merged, "Merged users (PG ∪ MySQL, deduplicated)")
     */
    } catch {
      case e: Exception =>
        println(s"❌ Critical error: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  /** Handles both regular files and Parquet folders */
  private def processItem(spark: SparkSession, item: File, cleanConfig: CleanConfig): Boolean = {
    try {
      println(s"Processing → ${item.getName}")

      val df = ExtractFlatFiles.read(spark, item.getAbsolutePath)
      ExtractFlatFiles.printContent(df)

      val cleanedDF = TransformClean.clean(df, cleanConfig)

      val outputPath = s"output/cleaned/${item.getName.replace(".", "_")}_cleaned"

      cleanedDF.write.mode("overwrite").parquet(outputPath)

      println(s"✅ Saved → $outputPath")
      true

    } catch {
      case e: Exception =>
        println(s"❌ Failed ${item.getName}: ${e.getMessage}")
        println("-" * 80)
        false
    }
  }

  def findItems(folderPath: String): Array[File] = {
    val rawDir = new File(folderPath)

    // check if folder exists and is a directory
    if (!rawDir.exists() || !rawDir.isDirectory) {
      println(s"❌ Folder '$folderPath' not found!")
      println("Please create the folder and put your files/folders inside.")
      return Array.empty[File]
    }

    // Improved filtering: exclude hidden/temp files
    val filesAndFolders = rawDir.listFiles()
      .filter(f => f.isFile || (f.isDirectory && f.getName.endsWith(".parquet")))
      .filter(f => !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      .sortBy(_.getName)

    // check if any files or folders found
    if (filesAndFolders.isEmpty) {
      println(s"⚠️ No files or Parquet folders found in '$folderPath'.")
      return Array.empty[File]
    }

    println(s"✅ Found ${filesAndFolders.length} item(s) in '$folderPath'\n")

    return filesAndFolders
  }

  def extractFlatFiles(spark: SparkSession, rawFolderPath: String, cleanConfig: CleanConfig): Unit = {
    var success = 0
    var failed = 0

    val filesAndFolders = findItems(rawFolderPath)

    filesAndFolders.foreach { item =>
      processItem(spark, item, cleanConfig) match {
        case true  => success += 1
        case false => failed += 1
      }
    }

    println(s"\n🎉 Processing completed! Success: $success | Failed: $failed")
  }
}