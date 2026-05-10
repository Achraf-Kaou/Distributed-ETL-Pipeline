package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import requests._

object ExtractApi {

  /**
   * Extract data from a REST API and return as DataFrame
   */
  def read(
    spark: SparkSession,
    url: String,
    method: String = "GET",
    params: Map[String, String] = Map.empty,
    headers: Map[String, String] = Map.empty,
    rootField: String = ""          // e.g. "data", "employees", "results"
  ): DataFrame = {

    println(s"Fetching data from API: $url")

    try {

      // Make the HTTP request
      val response = requests.get(
        url = url,
        params = params,
        headers = headers
      )

      // Check for successful response
      if (response.statusCode != 200) {
        throw new Exception(s"HTTP ${response.statusCode}: ${response.text()}")
      }

      // Get the response body as a string
      val jsonString = response.text()

      import spark.implicits._ // Import Spark implicits (needed for .toDS())

      // Create DataFrame from JSON string
      var df = spark.read
        .option("multiLine", "true")
        .json(Seq(jsonString).toDS())

      // Flatten if user specifies a root field (e.g. "employees")
      if (rootField.nonEmpty && df.columns.contains(rootField)) {
        df = df.select(explode(col(rootField)).as("record"))
               .select("record.*")
      }

      println(s"✅ Successfully fetched ${df.count()} records from API")
      df

    } catch {
      case e: Exception =>
        println(s"❌ API Error: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Example with pagination support (basic)
   */
  def readWithPagination(
    spark: SparkSession,
    baseUrl: String,
    pageParam: String = "page",
    pageSize: Int = 100
  ): DataFrame = {
    // You can extend this logic for multiple pages
    read(spark, s"$baseUrl?$pageParam=1")
  }

  def printContent(df: DataFrame, sourceName: String = "API Source"): Unit = {
    println(s"\n=== 🌐 $sourceName ===")
    df.printSchema()
    println("\nPreview:")
    df.show(50, truncate = false)
    println(s"Total rows: ${df.count()}")
    println("-" * 90)
  }
}