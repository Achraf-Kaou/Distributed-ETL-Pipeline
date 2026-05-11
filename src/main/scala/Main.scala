import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col
import extract.ExtractFlatFiles
import transform.{TransformClean, TransformDeduplicate, TransformJoin}
import transform.TransformClean.CleanConfig
import transform.TransformDeduplicate.DeduplicateConfig
import transform.TransformJoin.JoinConfig
import org.apache.spark.sql.types._
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Distributed ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val pipeline = new EtlPipeline(spark)
    
    try {
      pipeline.run()
    } catch {
      case e: Exception =>
        println(s"❌ Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

class EtlPipeline(spark: SparkSession) {

  private val rawFolderPath = "data/raw"
  private val outputBase    = "output"

  def run(): Unit = {
    println("=" * 90)
    println("🚀 Starting Distributed ETL Pipeline")
    println("=" * 90)

    // 1. Extract from ALL sources
    val rawDF = extractAllSources()

    if (rawDF.isEmpty) {
      println("⚠️ No data extracted. Exiting.")
      return
    }

    // 2. Clean
    val cleanedDF = clean(rawDF)

    // 3. Deduplicate
    val deduplicatedDF = deduplicate(cleanedDF)

    // 4. Join (Enrich with Departments)
    val enrichedDF = joinWithDepartments(deduplicatedDF)

    // 5. Save
    save(enrichedDF, "parquet")
    save(enrichedDF, "csv")

    println("\n🎉 Full ETL Pipeline completed successfully!")
  }

  /** Extract ALL files and union them with source tagging */
  private def extractAllSources(): DataFrame = {
    println("\n📥 Extraction Phase - Loading all sources")

    val items = findItems(rawFolderPath)
    if (items.isEmpty) return spark.emptyDataFrame

    val allDFs = items
      .filterNot(_.getName.toLowerCase.contains("department"))  // Skip departments - joined separately
      .map { item =>
        println(s"   Loading: ${item.getName}")
        
        var df = ExtractFlatFiles.read(spark, item.getAbsolutePath)
        
        // Normalize column names (lowercase, replace spaces with underscores)
        df = normalizeColumns(df)
        
        // Flatten JSON if it's nested
        if (item.getName.toLowerCase.contains("json")) {
          df = flattenJson(df)
        }

        // Tag source
        TransformDeduplicate.tag(df, detectSourceType(item.getName))
      }

    // Union all DataFrames with schema alignment
    allDFs.reduceOption(_ unionByName _).getOrElse(spark.emptyDataFrame)
  }

  /** Normalize column names to lowercase with underscores */
  private def normalizeColumns(df: DataFrame): DataFrame = {
    var result = df
    for (colName <- df.columns) {
      val normalizedName = colName.toLowerCase.replace(" ", "_").replace("-", "_")
      if (colName != normalizedName) {
        result = result.withColumnRenamed(colName, normalizedName)
      }
    }
    
    // Handle semantic column mapping
    val columnMapping = Map(
      "dept" -> "department",
      "startdate" -> "join_date",
      "location" -> "city"
    )
    
    for ((oldName, newName) <- columnMapping) {
      if (result.columns.contains(oldName)) {
        result = result.withColumnRenamed(oldName, newName)
      }
    }
    
    result
  }

  /** Flatten nested JSON structure from complex JSON */
  private def flattenJson(df: DataFrame): DataFrame = {
    if (df.columns.contains("personal_info")) {
      df
        .select(
          col("id"),
          col("personal_info.full_name").as("full_name"),
          col("personal_info.age").as("age"),
          col("personal_info.email").as("email"),
          col("employment.department").as("department"),
          col("employment.salary").as("salary"),
          col("employment.join_date").as("join_date"),
          col("employment.status").as("status"),
          col("address.city").as("city"),
          col("address.country").as("country")
        )
    } else {
      df
    }
  }

  private def detectSourceType(filename: String): String = {
    filename.toLowerCase match {
      case f if f.contains("department") => "departments"
      case f if f.contains("json")       => "api"
      case _                             => "file"
    }
  }

  private def clean(df: DataFrame): DataFrame = {
    println("\n🧹 Cleaning Phase")
    
    val cleanConfig = CleanConfig(
      criticalColumns = Seq("id", "full_name", "email"),   // Use normalized names
      fillValues = Map(
        "country"    -> "Unknown",
        "age"        -> "0",
        "salary"     -> "0",
        "department" -> "Unknown"
      ),
      stringColumns = Seq("full_name", "city", "country", "email", "department", "status"),
      castColumns = Map(
        "age"       -> IntegerType,
        "salary"    -> DoubleType,
        "join_date" -> DateType
      ),
      dropRowsWithNullsThreshold = 0.7,
      verbose = true
    )

    TransformClean.clean(df, cleanConfig)
  }

  private def deduplicate(df: DataFrame): DataFrame = {
    println("\n🔁 Deduplication Phase")

    if (!df.columns.contains("email")) {
      println("⚠️ Skipping deduplication (no 'email' column)")
      return df
    }

    val dedupConfig = DeduplicateConfig(
      keyColumns     = Seq("email"),
      recencyColumn  = Some("join_date"),
      sourcePriority = Map("file" -> 1, "api" -> 2, "departments" -> 3),
      sourceTag      = None,   // already tagged during extraction
      dropSourceCol  = true,
      verbose        = true
    )

    TransformDeduplicate.deduplicate(df, dedupConfig)
  }

  /** Join enriched example: Employees + Departments */
  private def joinWithDepartments(df: DataFrame): DataFrame = {
    println("\n🔗 Join Phase - Enriching with Departments")

    val departmentsPath = s"$rawFolderPath/departments.csv"
    val departmentsDF = ExtractFlatFiles.read(spark, departmentsPath)

    val joinConfig = JoinConfig(
      joinType      = "left",
      keyColumns    = Seq("department"),
      selectColumns = Seq(
        "id", "full_name", "age", "email", "salary", "join_date", "status",
        "department", "dept_name", "manager", "location", "budget"
      ),
      leftPrefix    = "",
      rightPrefix   = "dept_",
      verbose       = true
    )

    TransformJoin.join(df, departmentsDF, joinConfig)
  }

  private def save(df: DataFrame, format: String = "parquet"): Unit = {
    val timestamp = System.currentTimeMillis()
    val path = s"$outputBase/final/${timestamp}_final"

    format.toLowerCase match {
      case "csv" =>
        df.coalesce(1)
          .write.mode("overwrite")
          .option("header", "true")
          .csv(path)
      case _ =>
        df.write.mode("overwrite").parquet(path)
    }

    println(s"💾 Saved as $format → $path (${df.count()} rows)")
  }

  private def findItems(folderPath: String): Array[File] = {
    val dir = new File(folderPath)
    if (!dir.exists() || !dir.isDirectory) {
      println(s"❌ Folder not found: $folderPath")
      return Array.empty
    }

    dir.listFiles()
      .filter(f => f.isFile || (f.isDirectory && f.getName.endsWith(".parquet")))
      .filter(f => !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      .sortBy(_.getName)
  }
}