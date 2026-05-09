package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object ExtractFlatFiles {

  def read(
    spark: SparkSession,
    filePath: String,
    header: Boolean = true,
    delimiter: String = ",",
    sheetName: String = "Sheet1"
  ): DataFrame = {

    val ext = filePath.toLowerCase.split("\\.").last

    ext match {
      case "txt"          => readSimple(spark, filePath, header, delimiter)
      case "csv"          => readSimple(spark, filePath, header, delimiter)
      case "tsv"          => readSimple(spark, filePath, header, "\t")
      case "xlsx" | "xls" => readExcel(spark, filePath, sheetName, header)
      case "json"         => readJson(spark, filePath)
      case "parquet"      => readParquet(spark, filePath)
      case "xml"          => readXml(spark, filePath)
      case _              => throw new IllegalArgumentException(s"Unsupported file format: .$ext")
    }
  }

  // ===== CSV, TXT, TSV READER =====
  private def readSimple(spark: SparkSession, filePath: String, header: Boolean = true, delimiter: String ): DataFrame = {
    spark.read
      .option("header", header.toString())
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .option("mode", "PERMISSIVE")
      .option("encoding", "UTF-8")
      .csv(filePath)
  }

  // ===== XLSX READER =====
  private def readExcel(
    spark: SparkSession,
    filePath: String,
    sheetName: String,
    header: Boolean
  ): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header", header)
      .option("inferSchema", "true")
      .option("dataAddress", sheetName)   // Change if sheet name is different
      .load(filePath)
  }

  // ===== JSON READER =====
  private def readJson(spark: SparkSession, path: String): DataFrame = {
    val rawDF = spark.read
      .option("multiLine", "true")
      .option("mode", "PERMISSIVE")
      .json(path)

    if (rawDF.columns.contains("employees")) {
      rawDF
        .select(explode(col("employees")).as("employee"))
        .select("employee.*")
    } else {
      rawDF
    }
  }

  //  ==== PARQUET READER =====
  private def readParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  //  ==== XML READER =====
  private def readXml(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "row")
      .option("inferSchema", "true")
      .load(path)
  }

  def printContent(df: DataFrame): Unit = {
    println(s"\n=== Content of file ===")
    
    println("Schema:")
    df.printSchema()

    // Show all lines
    println("\nFirst rows of data:")
    df.show(100, truncate = false)
    
    println(s"Total lines: ${df.count()}")
  }

  def printSummary(df: DataFrame): Unit = {
    println("\nColumn Summary:")
    df.describe().show(truncate = false)
  }
}