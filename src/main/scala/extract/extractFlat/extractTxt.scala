package extract.extractFlat

import org.apache.spark.sql.{SparkSession, DataFrame}

object ExtractTxt {

  def read(spark: SparkSession, filePath: String, header: Boolean = true, delimiter: String ): DataFrame = {
    spark.read
      .option("header", header.toString())
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .option("mode", "PERMISSIVE")
      .option("encoding", "UTF-8")
      .csv(filePath)
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