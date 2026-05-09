package extract.extractFlat

import org.apache.spark.sql.{SparkSession, DataFrame}

object ExtractTxt {

  def read(spark: SparkSession, filePath: String, header: Boolean, delimiter: String ): DataFrame = {
    println(header.toString())
    spark.read
      .option("header", "true")      // Set to true if first line is header
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .csv(filePath)
  }

  def printContent(df: DataFrame, fileName: String): Unit = {
    println(s"\n=== Content of $fileName ===\n")
    
    println("Schema:")
    df.printSchema()

    // Show all lines
    println("\nFirst rows of data:")
    df.show(100, truncate = false)
    
    println(s"Total lines: ${df.count()}")
  }
}