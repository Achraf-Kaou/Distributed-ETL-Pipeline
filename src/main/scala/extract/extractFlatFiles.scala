package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

/**
 * ExtractFlatFiles — multi-format flat-file ingestion into Spark DataFrames.
 *
 * This object is the pipeline's first point of contact with file-based data.
 * It acts as a format router: the caller simply passes a file path, and the
 * correct reader is selected automatically based on the file extension.
 *
 * Supported formats and their notes:
 *
 * | Extension       | Reader            | Notes                                       |
 * |-----------------|-------------------|---------------------------------------------|
 * | `.csv` `.txt`   | Spark CSV         | Header-aware, UTF-8, schema inferred        |
 * | `.tsv`          | Spark CSV (tab)   | Tab delimiter forced automatically          |
 * | `.json`         | Spark JSON        | Multi-line; top-level `"employees"` array   |
 * |                 |                   | is auto-flattened via `explode`             |
 * | `.parquet`      | Spark Parquet     | Column pruning & predicate pushdown apply  |
 * | `.xlsx` `.xls`  | spark-excel lib   | Sheet1 by default; schema inferred          |
 * | `.xml`          | spark-xml lib     | `rowTag = "row"` by default                 |
 *
 * All readers set `mode = PERMISSIVE` where supported, meaning malformed rows
 * are silently included with nulls rather than causing the job to abort. The
 * QuarantineHandler downstream will isolate such rows if critical columns are null.
 *
 * Design note — why a single `read()` dispatcher instead of separate methods per format:
 * The calling code ([[Main.EtlPipeline.extractFlatFiles]]) iterates over a directory
 * of heterogeneous files. A single entry point that auto-detects format keeps that
 * loop simple and extensible — adding a new format only requires one new case here.
 */
object ExtractFlatFiles {

  /**
   * Read a single flat file and return its contents as a Spark DataFrame.
   *
   * The format is determined from the lowercase file extension. If the extension
   * is not recognised, an [[IllegalArgumentException]] is thrown so the caller
   * can decide to skip or abort.
   *
   * @param spark      Active [[SparkSession]].
   * @param filePath   Absolute or relative path to the file.
   * @param header     Whether the first row is a header (applies to CSV/TSV/TXT).
   *                   Defaults to `true`.
   * @param delimiter  Field delimiter for CSV/TXT reads. Defaults to `","`.
   *                   TSV files override this with `"\t"` regardless of what is passed.
   * @param sheetName  Excel sheet name to read. Defaults to `"Sheet1"`.
   * @return           DataFrame with inferred schema and one row per data record.
   * @throws IllegalArgumentException if the file extension is not supported.
   */
  def read(
    spark:     SparkSession,
    filePath:  String,
    header:    Boolean = true,
    delimiter: String  = ",",
    sheetName: String  = "Sheet1"
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

  // ─────────────────────────────────────────────────────────────────
  // Format-specific readers (private)
  // ─────────────────────────────────────────────────────────────────

  /**
   * Read a delimited text file (CSV, TSV, TXT) using the Spark CSV reader.
   *
   * Key options:
   *   - `inferSchema = true` — Spark samples rows to determine column types.
   *     This is acceptable for development; production pipelines should supply
   *     an explicit schema to avoid sampling overhead and type surprises.
   *   - `mode = PERMISSIVE` — malformed records are included with nulls in the
   *     `_corrupt_record` column rather than failing the job.
   *   - `encoding = UTF-8` — explicit to prevent locale-dependent defaults on
   *     some cluster configurations.
   */
  private def readSimple(
    spark:     SparkSession,
    filePath:  String,
    header:    Boolean = true,
    delimiter: String
  ): DataFrame = {
    spark.read
      .option("header",      header.toString())
      .option("inferSchema", "true")
      .option("delimiter",   delimiter)
      .option("mode",        "PERMISSIVE")
      .option("encoding",    "UTF-8")
      .csv(filePath)
  }

  /**
   * Read an Excel workbook using the `spark-excel` community library.
   *
   * The `dataAddress` option specifies the sheet name to read.
   * Change this if your Excel file uses a different sheet name than "Sheet1".
   *
   * Dependency: `com.crealytics %% spark-excel` (declared in build.sbt).
   */
  private def readExcel(
    spark:     SparkSession,
    filePath:  String,
    sheetName: String,
    header:    Boolean
  ): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .option("header",      header)
      .option("inferSchema", "true")
      .option("dataAddress", sheetName)
      .load(filePath)
  }

  /**
   * Read a JSON file, with special handling for the `"employees"` root array.
   *
   * The Spark JSON reader with `multiLine = true` expects either:
   *   a) A single JSON object per file, or
   *   b) A JSON array at the root level.
   *
   * When the JSON contains a top-level `"employees"` key whose value is an array
   * (a common API response envelope pattern), this method flattens it using
   * `explode` so each array element becomes its own row. If no such key exists,
   * the raw DataFrame is returned as-is.
   *
   * The struct columns produced by `explode` are subsequently flattened into
   * scalar columns by [[Main.EtlPipeline.flattenStructs]] in the main pipeline.
   */
  private def readJson(spark: SparkSession, path: String): DataFrame = {
    val rawDF = spark.read
      .option("multiLine", "true")
      .option("mode",      "PERMISSIVE")
      .json(path)

    if (rawDF.columns.contains("employees")) {
      rawDF
        .select(explode(col("employees")).as("employee"))
        .select("employee.*")
    } else {
      rawDF
    }
  }

  /**
   * Read a Parquet file or directory.
   *
   * Parquet is the preferred format for intermediate pipeline data because Spark
   * can push down column selection and row filters into the file scan itself
   * (predicate pushdown), reducing the volume of data read from disk significantly.
   */
  private def readParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  /**
   * Read an XML file using the `spark-xml` community library.
   *
   * The `rowTag` option tells the parser which XML element represents a single
   * data record. Default is `"row"` — change this for other XML schemas.
   *
   * Dependency: `com.databricks %% spark-xml` (declared in build.sbt).
   */
  private def readXml(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag",      "row")
      .option("inferSchema", "true")
      .load(path)
  }

  // ─────────────────────────────────────────────────────────────────
  // Development / debugging utilities
  // ─────────────────────────────────────────────────────────────────

  /**
   * Print schema, first 100 rows, and total row count to stdout.
   * Intended for interactive development and debugging only — do not call
   * this in production pipeline code paths.
   *
   * @param df  DataFrame to inspect.
   */
  def printContent(df: DataFrame): Unit = {
    println(s"\n=== Content of file ===")
    println("Schema:")
    df.printSchema()
    println("\nFirst rows of data:")
    df.show(100, truncate = false)
    println(s"Total lines: ${df.count()}")
  }

  /**
   * Print a statistical summary (min, max, mean, stddev, count) for all columns.
   * Useful for quickly assessing the range and distribution of values in a source file.
   *
   * @param df  DataFrame to summarise.
   */
  def printSummary(df: DataFrame): Unit = {
    println("\nColumn Summary:")
    df.describe().show(truncate = false)
  }
}