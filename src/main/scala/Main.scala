import org.apache.spark.sql.{SparkSession, DataFrame}
import extract.{ExtractFlatFiles, ExtractApi}
import transform.{TransformClean, TransformDeduplicate}
import transform.TransformClean.CleanConfig
import transform.TransformDeduplicate.DeduplicateConfig
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

// ===================================================================
// ETL Pipeline Orchestrator
// ===================================================================

class EtlPipeline(spark: SparkSession) {

  private val rawFolderPath = "data/raw"
  private val outputBase    = "output"

  def run(): Unit = {
    println("=" * 90)
    println("🚀 Starting Distributed ETL Pipeline")
    println("=" * 90)

    // 1. Extract
    val rawDF = extract()

    if (rawDF.isEmpty) {
      println("⚠️ No data extracted. Exiting.")
      return
    }

    // 2. Clean
    val cleanedDF = clean(rawDF)

    // 3. Deduplicate
    val finalDF = deduplicate(cleanedDF)

    // 4. Save
    save(finalDF, format = "parquet")
    save(finalDF, format = "csv")

    println("\n🎉 ETL Pipeline completed successfully!")
  }

  private def extract(): DataFrame = {
    println("\n📥 Extraction Phase")
    val items = findItems(rawFolderPath)
    
    if (items.isEmpty) return spark.emptyDataFrame

    // For now: process first file (you can extend to union multiple)
    val firstItem = items.head
    println(s"Processing: ${firstItem.getName}")
    
    ExtractFlatFiles.read(spark, firstItem.getAbsolutePath)
  }

  private def clean(df: DataFrame): DataFrame = {
    println("\n🧹 Cleaning Phase")
    
    val cleanConfig = CleanConfig(
      criticalColumns = Seq("id", "name", "email"),
      fillValues = Map(
        "country"    -> "Unknown",
        "age"        -> "0",
        "salary"     -> "0",
        "department" -> "Unknown"
      ),
      stringColumns = Seq("name", "city", "country", "email", "department", "status"),
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
      println("⚠️ Skipping deduplication (no 'email' column found)")
      return df
    }

    val dedupConfig = DeduplicateConfig(
      keyColumns     = Seq("email"),
      recencyColumn  = Some("join_date"),
      sourcePriority = Map("file" -> 1, "api" -> 2, "postgres" -> 3),
      sourceTag      = Some("file"),
      dropSourceCol  = true,
      verbose        = true
    )

    val taggedDF = TransformDeduplicate.tag(df, "file")
    TransformDeduplicate.deduplicate(taggedDF, dedupConfig)
  }

  private def save(df: DataFrame, format: String = "parquet"): Unit = {
    val outputPath = s"$outputBase/final/${System.currentTimeMillis()}_final"
    format.toLowerCase match {
      case "csv" =>
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .option("delimiter", ",")
          .csv(outputPath)
        
      case "parquet" | _ =>
        df.write.mode("overwrite").parquet(outputPath)
    }
    
    println(s"💾 Data saved as $format → $outputPath")
  }

  // Helper methods
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