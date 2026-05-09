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

    val filePath = "data/raw/example.txt"

    try {

      // Read the TXT file using Spark
      val df = ExtractTxt.read(spark, filePath, header = false, delimiter = ",")
      println("============= Raw DataFrame =============")
      df.collect().foreach(row => println(row.getString(0))) // Print raw lines
      
      // Print the content
      ExtractTxt.printContent(df, filePath)

      // You can now use the data like this:
      println("\n=== Examples of using the data ===")
      
      // Example 1: Show only specific columns
      df.select("Name", "Age", "City").show(5)

      // Example 2: Filter data
      df.filter("Age > 30").show()

      // Example 3: Count by City
      df.groupBy("City").count().show()

    } catch {
      case e: Exception =>
        println(s"Error reading file: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

}