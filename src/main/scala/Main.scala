import org.apache.spark.sql.SparkSession
import extract.ExtractFlatFiles
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder()
      .appName("Distributed ETL - Raw Data Processor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawFolderPath = "data/raw"

    try {
      val rawDir = new File(rawFolderPath)

      if (!rawDir.exists() || !rawDir.isDirectory) {
        println(s"❌ Folder '$rawFolderPath' not found!")
        println("Please create the folder and put your files/folders inside.")
        return
      }

      // Improved filtering: exclude hidden/temp files
      val filesAndFolders = rawDir.listFiles()
        .filter(f => f.isFile || (f.isDirectory && f.getName.endsWith(".parquet")))
        .filter(f => !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        .sortBy(_.getName)

      if (filesAndFolders.isEmpty) {
        println(s"⚠️ No files or Parquet folders found in '$rawFolderPath'.")
        return
      }

      println(s"✅ Found ${filesAndFolders.length} item(s) in '$rawFolderPath'\n")

      var success = 0
      var failed = 0

      filesAndFolders.foreach { item =>
        processItem(spark, item) match {
          case true  => success += 1
          case false => failed += 1
        }
      }

      println(s"\n🎉 Processing completed! Success: $success | Failed: $failed")

    } catch {
      case e: Exception =>
        println(s"❌ Critical error: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  /** Handles both regular files and Parquet folders */
  private def processItem(spark: SparkSession, item: File): Boolean = {
    try {
      println(s"Processing → ${item.getName}")

      val df = ExtractFlatFiles.read(spark, item.getAbsolutePath)
      ExtractFlatFiles.printContent(df)
      true

    } catch {
      case e: Exception =>
        println(s"❌ Failed ${item.getName}: ${e.getMessage}")
        println("-" * 80)
        false
    }
  }
}