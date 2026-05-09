import org.apache.spark.sql.SparkSession
import extract.extractFlat.ExtractTxt
import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
      
    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("TXT Extractor with Spark")
      .master("local[*]")           // Run locally
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rawFolderPath = "data/raw"

    try {

      val rawDir = new File(rawFolderPath)

      if (!rawDir.exists() || !rawDir.isDirectory) {
        println(s"❌ Folder '$rawFolderPath' not found!")
        println("Please create the folder and put your files inside it.")
        return
      }

      val files = rawDir.listFiles()
        .filter(_.isFile)
        .sortBy(_.getName)

      if (files.isEmpty) {
        println(s"⚠️ No files found in '$rawFolderPath' folder.")
        return
      }

      println(s"✅ Found ${files.length} file(s) in '$rawFolderPath'\n")

      files.foreach { file =>
        processFile(spark, file)
      }

      println("🎉 All files processed successfully!")

    } catch {
      case e: Exception =>
        println(s"Error reading file: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  def processFile(spark: SparkSession, file: File): Unit = {
    try {
      println(s"Processing file: ${file.getName}")

      val df = ExtractTxt.read(
        spark = spark, 
        filePath = file.getAbsolutePath, 
        header = true, 
        delimiter = ","
      )

      ExtractTxt.printContent(df)
      ExtractTxt.printSummary(df)
    } catch {
      case e: Exception =>
        println(s"Error processing file '${file.getName}': ${e.getMessage}")
    }
  }

}