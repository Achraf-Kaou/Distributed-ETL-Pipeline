package extract

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import requests._

/**
 * ExtractApi — REST API ingestion into Spark DataFrames.
 *
 * This object provides HTTP-based data extraction for the ETL pipeline.
 * It uses the `com.lihaoyi:requests` library for synchronous HTTP calls,
 * parses the JSON response on the driver, and then parallelises the result
 * into Spark by creating a single-element Dataset from the response string
 * and letting Spark's JSON reader distribute parsing.
 *
 * Architecture note — driver-side HTTP call:
 * The HTTP request runs on the Spark driver, not on executor nodes. This is
 * intentional for small-to-medium API payloads (< a few MB). For paginated APIs
 * or large response sets, a partitioned fetch strategy should be implemented
 * (see [[readWithPagination]] as a starting point).
 *
 * The response is ingested via `spark.read.json(Seq(jsonString).toDS())`.
 * This pattern avoids writing the response to disk: the JSON string lives in
 * driver memory, is broadcast as a single-partition Dataset, and Spark parses
 * it using its built-in JSON reader with schema inference.
 *
 * Root-field flattening:
 * Many REST APIs wrap their payload in an envelope object:
 * {{{ { "data": [ {...}, {...} ] } }}}
 * The `rootField` parameter handles this by applying `explode` on the named
 * array, turning each element into its own row. Nested struct columns are
 * subsequently flattened by [[Main.EtlPipeline.flattenStructs]].
 */
object ExtractApi {

  /**
   * Fetch data from a REST API endpoint and return a Spark DataFrame.
   *
   * Steps:
   *   1. Execute an HTTP GET or POST request with the given params and headers.
   *   2. Validate the HTTP status code (must be 200).
   *   3. Parse the JSON response body into a Spark DataFrame via a single-element Dataset.
   *   4. If `rootField` is set and the column exists, explode the array to produce one row
   *      per array element; otherwise return the flat top-level structure.
   *
   * @param spark      Active [[SparkSession]].
   * @param url        Full URL of the API endpoint, e.g. `"https://api.example.com/users"`.
   * @param method     HTTP method: `"GET"` (default) or `"POST"`. Case-insensitive.
   * @param params     Query string parameters appended to the URL. Defaults to empty.
   * @param headers    HTTP request headers (e.g. Authorization, Content-Type). Defaults to empty.
   * @param rootField  Optional top-level JSON key whose value is the array to flatten.
   *                   Example: `"employees"` for `{ "employees": [ {...} ] }`.
   *                   Leave empty (`""`) if the response is a JSON array at the root.
   * @return           DataFrame with one row per API record.
   * @throws Exception on non-200 HTTP status or network failure. Callers in
   *                   [[Main.EtlPipeline.extractApis]] catch and optionally skip this.
   */
  def read(
    spark:     SparkSession,
    url:       String,
    method:    String              = "GET",
    params:    Map[String, String] = Map.empty,
    headers:   Map[String, String] = Map.empty,
    rootField: String              = ""
  ): DataFrame = {

    println(s"Fetching data from API: $url")

    try {
      // ── Step 1: Execute the HTTP request ──────────────────────────────────
      val response = method.trim.toUpperCase match {
        case "GET" =>
          requests.get(url = url, params = params, headers = headers)
        case "POST" =>
          requests.post(url = url, params = params, headers = headers)
        case unsupported =>
          throw new IllegalArgumentException(
            s"Unsupported API method '$unsupported'. Supported methods: GET, POST."
          )
      }

      // ── Step 2: Validate HTTP status ──────────────────────────────────────
      if (response.statusCode != 200) {
        throw new Exception(s"HTTP ${response.statusCode}: ${response.text()}")
      }

      val jsonString = response.text()

      import spark.implicits._

      // ── Step 3: Parse JSON into a Spark DataFrame ─────────────────────────
      // Wrapping the JSON string in a single-element Dataset avoids writing to
      // disk. Spark distributes the parse across its JSON reader in a
      // single-partition RDD. For very large responses, consider writing the
      // string to a temp path on HDFS/S3 first and reading it as a file.
      var df = spark.read
        .option("multiLine", "true")
        .json(Seq(jsonString).toDS())

      // ── Step 4: Flatten root-level array (envelope pattern) ───────────────
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
   * Basic paginated API reader — starting point for multi-page ingestion.
   *
   * This is a stub implementation that reads page 1 only. Extend it with a
   * loop that increments `pageParam` until an empty response or a total count
   * is reached, then union all pages into a single DataFrame.
   *
   * @param spark      Active [[SparkSession]].
   * @param baseUrl    Base API URL without pagination parameters.
   * @param pageParam  Query parameter name for the page number. Defaults to `"page"`.
   * @param pageSize   Number of records per page (for reference — extend loop logic).
   * @return           DataFrame from page 1 of the API.
   */
  def readWithPagination(
    spark:     SparkSession,
    baseUrl:   String,
    pageParam: String = "page",
    pageSize:  Int    = 100
  ): DataFrame = {
    // TODO: Extend this with a while-loop that increments the page parameter,
    // unions partial DataFrames, and stops when the response is empty or a
    // total-count header is exhausted.
    read(spark, s"$baseUrl?$pageParam=1")
  }

  /**
   * Print schema, preview rows, and total count for an API-sourced DataFrame.
   * For interactive development and debugging only.
   *
   * @param df          DataFrame returned by [[read]].
   * @param sourceName  Display label shown in the output header.
   */
  def printContent(df: DataFrame, sourceName: String = "API Source"): Unit = {
    println(s"\n=== 🌐 $sourceName ===")
    df.printSchema()
    println("\nPreview:")
    df.show(50, truncate = false)
    println(s"Total rows: ${df.count()}")
    println("-" * 90)
  }
}
