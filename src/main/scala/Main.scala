import org.apache.spark.sql.SparkSession
import extract.extractFlat.ExtractTxt

object Main {

  def main(args: Array[String]): Unit = {
      
    // Create Spark Session
    val spark = SparkSession.builder()
      .appName("TXT Extractor with Spark")
      .master("local[*]")           // Run locally
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {

      // Read the TXT file using Spark
      val df = ExtractTxt.read(
        spark = spark, 
        filePath = "data/raw/example.txt", 
        header = true, 
        delimiter = ","
      )

      ExtractTxt.printContent(df)
      ExtractTxt.printSummary(df)

    } catch {
      case e: Exception =>
        println(s"Error reading file: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

}