import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import java.io.File

import audit.AuditLogger
import java.time.Instant
import config.PipelineConfig
import config.PipelineConfig.{AppConfig, DatabaseSourceConfig}
import extract.{ExtractApi, ExtractDatabase, ExtractFlatFiles}
import quality.QuarantineHandler
import load.{StarSchemaBuilder, WarehouseLoader}
import logging.PipelineLogger
import orchestration.PipelineOrchestrator
import transform.QualityChecks
import transform.{TransformClean, TransformDeduplicate, TransformJoin, TransformAggregate}

/**
 * Main — application entry point.
 *
 * Responsibilities:
 *   1. Load [[config.PipelineConfig]] from `application.conf` (or a custom path).
 *   2. Build and configure the [[SparkSession]] using config-driven settings.
 *   3. Instantiate [[EtlPipeline]] and call [[EtlPipeline.run]].
 *   4. Guarantee `spark.stop()` is called in the `finally` block regardless of
 *      success or failure, releasing cluster resources cleanly.
 *
 * Spark log level is set to ERROR to suppress verbose INFO/WARN messages from
 * internal Spark components. The pipeline's own log output uses SLF4J/Log4j 2
 * via [[logging.PipelineLogger]] and is controlled separately by `log4j2.xml`.
 */
object Main {

  def main(args: Array[String]): Unit = {
    // Load config first because SparkSession settings (app name/master/partitions) come from it.
    val config = PipelineConfig.load()

    val spark = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions.toString)
      .getOrCreate()

    // Suppress Spark's internal INFO/WARN noise; pipeline logs are handled by Log4j 2.
    spark.sparkContext.setLogLevel("ERROR")

    val pipeline = new EtlPipeline(spark, config)
    
    try {
      pipeline.run()
    } catch {
      case e: Exception =>
        println(s"❌ Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}

/**
 * EtlPipeline — top-level orchestration class that coordinates all ETL stages.
 *
 * Each stage is executed through [[orchestration.PipelineOrchestrator.runStage]],
 * which handles logging, timing, audit recording, and fail-fast/skip logic.
 *
 * == Stage Execution Order ==
 * {{{
 *   extract       → extractAllSources()       → raw unified DataFrame
 *   clean         → clean()                   → null handling, type casting, normalisation
 *   dedup         → deduplicate()             → Window-based business-key deduplication
 *   join          → applyConfiguredJoins()    → left-join with dimension/enrichment sources
 *   aggregate     → aggregate()               → GROUP BY summaries
 *   build_star    → StarSchemaBuilder.build() → dim + fact DataFrames
 *   load          → save() + WarehouseLoader  → Parquet/CSV output + JDBC upsert
 * }}}
 *
 * Any stage can be selectively disabled by removing its name from
 * `etl.orchestration.stages` in `application.conf`.
 *
 * @param spark      Active [[SparkSession]] created in [[Main]].
 * @param appConfig  Fully parsed [[config.PipelineConfig.AppConfig]].
 */
class EtlPipeline(spark: SparkSession, appConfig: AppConfig) {

  private val outputBase = appConfig.load.outputBasePath
  private val logger = new PipelineLogger(appConfig.logging.metricsEnabled)
  // generated run id for this pipeline invocation
  private val runId: String = java.util.UUID.randomUUID().toString
  // simple file-based audit logger (directory ensured inside)
  private val auditLogger = new AuditLogger(s"$outputBase/final/warehouse/audit.log")
  private val quarantineHandler = new QuarantineHandler(spark, logger)

  private val orchestrator = new PipelineOrchestrator(
    appConfig.orchestration.stages,
    appConfig.orchestration.failFast,
    logger,
    auditLogger,
    runId
  )

  /**
   * Execute the full ETL pipeline end-to-end.
   *
   * Delegates each stage to [[orchestration.PipelineOrchestrator.runStage]].
   * Stages receive the output of the previous stage via `getOrElse` fallbacks —
   * if a stage is skipped or fails (failFast=false), the previous DataFrame is
   * passed through so subsequent stages can still run on best-effort data.
   *
   * A run summary is written to the audit log in both success and failure paths.
   */
  def run(): Unit = {
    val startTime = Instant.now().toString

    logger.info("=" * 90)
    logger.info("Starting Distributed ETL Pipeline - Run ID: " + runId)
    logger.info("=" * 90)
    try {
      val rawDF = orchestrator.runStage("extract") { extractAllSources() }.getOrElse(spark.emptyDataFrame)

      val extractedCount = rawDF.count()

      if (rawDF.isEmpty) {
        logger.warn("No data extracted. Exiting.")
        return
      }

      val cleanedDF = orchestrator.runStage("clean", extractedCount) { clean(rawDF) }.getOrElse(rawDF)
      runQualityChecks(cleanedDF, "clean")

      val deduplicatedDF = orchestrator.runStage("dedup", extractedCount) { deduplicate(cleanedDF) }.getOrElse(cleanedDF)
      runQualityChecks(deduplicatedDF, "dedup")

      val enrichedDF = orchestrator.runStage("join", extractedCount) { applyConfiguredJoins(deduplicatedDF) }.getOrElse(deduplicatedDF)

      val aggregatedDF = orchestrator.runStage("aggregate", extractedCount) { aggregate(enrichedDF) }.getOrElse(enrichedDF)

      val starSchema = orchestrator.runStage("build_star", extractedCount) {
        StarSchemaBuilder.build(spark, enrichedDF, appConfig.warehouse, logger.warn)
      }

      orchestrator.runStage("load") {
        appConfig.load.enabledFormats.foreach(fmt => save(aggregatedDF, fmt))
        starSchema.foreach { star =>
          WarehouseLoader.loadStarSchema(star, appConfig.warehouse, appConfig.load.warehouseTarget, appConfig.load.upsert)
        }
      }

      logger.info("Full ETL Pipeline completed successfully.")

      auditLogger.recordRunSummary(
        runId          = runId,
        startTime      = startTime,
        endTime        = Instant.now().toString,
        status         = "SUCCESS",
        totalExtracted = extractedCount,
        totalLoaded    = 0 // you can track this too
      )
    } catch {
      case e: Exception =>
        auditLogger.recordRunSummary(runId, startTime, Instant.now().toString, "FAILED", 0, 0)
        throw e
    }
  }

  /**
   * Union all enabled sources into a single DataFrame.
   *
   * Sources are extracted in this order: flat files → APIs → databases.
   * `unionByName(allowMissingColumns = true)` aligns columns by name rather than
   * position and fills missing columns with null, making the union schema-agnostic.
   * This is essential because different sources (CSV, JSON, DB) rarely have identical schemas.
   *
   * @return  Combined DataFrame from all enabled sources, or an empty DataFrame if no
   *          source produced any data.
   */
  private def extractAllSources(): DataFrame = {
    println("\n📥 Extraction Phase - Loading all sources")

    val extracted = extractFlatFiles() ++ extractApis() ++ extractDatabases()
    extracted.reduceOption(_.unionByName(_, allowMissingColumns = true)).getOrElse(spark.emptyDataFrame)
  }

  /**
   * Read flat files from the configured directory, excluding files whose names match
   * any string in `exclude-name-contains` (e.g. `"departments"` is excluded because
   * it is used as a join dimension, not a primary data source).
   *
   * Each file is:
   *   1. Read via [[extract.ExtractFlatFiles.read]] (format auto-detected by extension).
   *   2. Struct columns are flattened to scalars via [[flattenStructs]].
   *   3. Column names are normalised to snake_case and mapped via `column-mapping` config.
   *   4. Tagged with a `__source` label (e.g. `"file_csv"`) for dedup priority.
   */
  private def extractFlatFiles(): Seq[DataFrame] = {
    if (!appConfig.extract.flatFiles.enabled) return Seq.empty

    val items = findItems(appConfig.extract.flatFiles.path)
    if (items.isEmpty) return Seq.empty

    val excludes = appConfig.extract.flatFiles.excludeNameContains.map(_.toLowerCase)
    items
      .filterNot(item => excludes.exists(ex => item.getName.toLowerCase.contains(ex)))
      .map { item =>
        println(s"   Loading flat file: ${item.getName}")
        val raw = ExtractFlatFiles.read(spark, item.getAbsolutePath)
        val prepared = normalizeAndMap(flattenStructs(raw))
        val sourceTag = s"file_${extractExtension(item.getName)}"
        TransformDeduplicate.tag(prepared, sourceTag)
      }
      .toSeq
  }

  /**
   * Fetch data from all enabled REST API sources defined in `extract.apis`.
   *
   * Each API source is attempted independently. If `skip-non-critical-source-failures = true`
   * in orchestration config, a failed HTTP call is logged as a warning and the source
   * is skipped rather than aborting the entire extract stage. This is the recommended
   * setting for optional enrichment APIs that may be temporarily unavailable.
   */
  private def extractApis(): Seq[DataFrame] = {
    appConfig.extract.apis
      .filter(_.enabled)
      .flatMap { apiCfg =>
        try {
          logger.info(s"Loading API source: ${apiCfg.name}")
          val raw = ExtractApi.read(
            spark = spark,
            url = apiCfg.url,
            method = apiCfg.method,
            params = apiCfg.params,
            headers = apiCfg.headers,
            rootField = apiCfg.rootField
          )
          val prepared = normalizeAndMap(flattenStructs(raw))
          Some(TransformDeduplicate.tag(prepared, apiCfg.sourceTag))
        } catch {
          case e: Exception =>
            if (appConfig.orchestration.skipNonCriticalSourceFailures) {
              logger.warn(s"API source '${apiCfg.name}' skipped: ${e.getMessage}")
              None
            } else {
              throw e
            }
        }
      }
  }

  /**
   * Read data from all enabled JDBC database sources defined in `extract.databases`.
   *
   * Same fault-tolerance pattern as [[extractApis]]: individual source failures
   * are caught and optionally skipped so a single unavailable database does not
   * abort extraction from all other sources.
   *
   * Partitioned reads (when `partition-column` is configured) spawn multiple parallel
   * Spark tasks — one JDBC connection per partition. Ensure the target database can
   * handle `num-partitions` simultaneous connections.
   */
  private def extractDatabases(): Seq[DataFrame] = {
    appConfig.extract.databases
      .filter(_.enabled)
      .flatMap { dbCfg =>
        try {
          logger.info(s"Loading DB source: ${dbCfg.name}")
          val raw = readFromDbConfig(dbCfg, dbCfg.tableOrQuery)
          val prepared = normalizeAndMap(flattenStructs(raw))
          Some(TransformDeduplicate.tag(prepared, dbCfg.sourceTag))
        } catch {
          case e: Exception =>
            if (appConfig.orchestration.skipNonCriticalSourceFailures) {
              logger.warn(s"DB source '${dbCfg.name}' skipped: ${e.getMessage}")
              None
            } else {
              throw e
            }
        }
      }
  }

  /**
   * Normalise column names to snake_case and apply the `column-mapping` aliases.
   *
   * Two-step process:
   *   1. If `normalize-columns = true`, convert every column name to snake_case
   *      using [[transform.TransformClean.toSnakeCase]].
   *   2. Apply explicit renames from `column-mapping` in config
   *      (e.g. `dept → department`, `startdate → join_date`).
   *      The mapping keys are also normalised before comparison so
   *      `"StartDate"` in config correctly matches `"startdate"` in the DataFrame.
   *
   * @param df  DataFrame with raw column names from the source system.
   * @return    DataFrame with normalised and renamed column names.
   */
  private def normalizeAndMap(df: DataFrame): DataFrame = {
    val base =
      if (appConfig.transform.normalizeColumns) {
        df.columns.foldLeft(df) { (acc, c) =>
          val normalized = TransformClean.toSnakeCase(c)
          if (normalized == c) acc else acc.withColumnRenamed(c, normalized)
        }
      } else df

    val normalizedMapping = appConfig.transform.columnMapping.map {
      case (from, to) => TransformClean.toSnakeCase(from) -> TransformClean.toSnakeCase(to)
    }

    normalizedMapping.foldLeft(base) { case (acc, (from, to)) =>
      if (acc.columns.contains(from) && from != to) acc.withColumnRenamed(from, to)
      else acc
    }
  }

  /**
   * Recursively flatten all StructType columns into scalar columns.
   *
   * Spark's JSON and API readers produce nested StructType columns when the source
   * data contains nested objects (e.g. `{ "address": { "city": "Paris" } }` becomes
   * a `StructType` column `address` with a sub-field `city`).
   *
   * This method iterates until no StructType fields remain:
   *   - Each struct field `parent.child` becomes `parent_child` (underscore-joined).
   *   - Non-struct columns are passed through unchanged.
   *   - Deeply nested structs (struct inside struct) are handled by repeated passes.
   *
   * WHY flatten before normalising: column name normalisation and mappings operate on
   * flat string names. Flattening first ensures those steps see the full dot-path
   * name converted to a valid identifier.
   *
   * @param df  DataFrame potentially containing StructType columns.
   * @return    DataFrame with all struct columns expanded into scalar columns.
   */
  private def flattenStructs(df: DataFrame): DataFrame = {
    var current = df
    var hasStruct = true

    while (hasStruct) {
      val structFields = current.schema.fields.collect {
        case f if f.dataType.isInstanceOf[StructType] => f.name
      }

      if (structFields.isEmpty) {
        hasStruct = false
      } else {
        val projected = current.schema.fields.flatMap { field =>
          field.dataType match {
            case s: StructType =>
              s.fieldNames.toSeq.map(child => col(s"${field.name}.$child").as(s"${field.name}_$child"))
            case _ =>
              Seq(col(field.name))
          }
        }
        current = current.select(projected: _*)
      }
    }

    current
  }

  /**
   * Resolve a [[config.PipelineConfig.DatabaseSourceConfig]] into the appropriate
   * [[extract.ExtractDatabase.DbConfig]] subtype and execute the read.
   *
   * Supports partitioned reads when `partition-column`, `lower-bound`, and
   * `upper-bound` are all configured. Falls back to a single-partition read
   * if any of those three values is absent.
   *
   * @param dbCfg        Database source configuration from `extract.databases`.
   * @param tableOrQuery Table name or SQL subquery to read.
   * @return             DataFrame from the database source.
   * @throws IllegalArgumentException for unsupported `db-type` values.
   */
  private def readFromDbConfig(dbCfg: DatabaseSourceConfig, tableOrQuery: String): DataFrame = {
    val typedCfg = dbCfg.dbType.trim.toLowerCase match {
      case "postgres" | "postgresql" =>
        ExtractDatabase.PostgresConfig(
          host = dbCfg.host,
          port = if (dbCfg.port > 0) dbCfg.port else 5432,
          database = dbCfg.database,
          user = dbCfg.user,
          password = dbCfg.password
        )
      case "mysql" =>
        ExtractDatabase.MySQLConfig(
          host = dbCfg.host,
          port = if (dbCfg.port > 0) dbCfg.port else 3306,
          database = dbCfg.database,
          user = dbCfg.user,
          password = dbCfg.password
        )
      case "sqlite" =>
        ExtractDatabase.SQLiteConfig(dbCfg.sqliteFilePath)
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported db-type '$other' for source '${dbCfg.name}'. Supported: postgres, mysql, sqlite."
        )
    }

    (dbCfg.partitionColumn, dbCfg.lowerBound, dbCfg.upperBound) match {
      case (Some(partitionCol), Some(lower), Some(upper)) =>
        ExtractDatabase.readPartitioned(
          spark = spark,
          config = typedCfg,
          table = tableOrQuery,
          partitionColumn = partitionCol,
          lowerBound = lower,
          upperBound = upper,
          numPartitions = dbCfg.numPartitions
        )
      case _ =>
        ExtractDatabase.read(spark, typedCfg, tableOrQuery)
    }
  }

  /**
   * Execute the full 7-step cleaning chain via [[transform.TransformClean.clean]].
   *
   * Before cleaning, bad rows (nulls in critical columns) are quarantined via
   * [[quality.QuarantineHandler]] so they are preserved for investigation while
   * being excluded from the clean dataset.
   *
   * Returns the input DataFrame unchanged if `clean.enabled = false` in config.
   *
   * @param df  Raw or lightly preprocessed DataFrame from the extract stage.
   * @return    Cleaned DataFrame ready for deduplication.
   */
  private def clean(df: DataFrame): DataFrame = {
    logger.info("Cleaning phase started")

    if (!appConfig.transform.clean.enabled) {
      logger.info("Cleaning phase is disabled in config")
      return df
    }

    val cleanCfg = appConfig.transform.clean
    val runId = this.runId 

    // === QUARANTINE BAD ROWS BEFORE CLEANING ===
    val quarantineHandler = new quality.QuarantineHandler(spark, logger)

    var cleaned = df

    // 1. Quarantine rows with critical nulls (most important)
    if (cleanCfg.criticalColumns.nonEmpty) {
      cleaned = quarantineHandler.quarantineAndClean(
        df = cleaned,
        runId = runId,
        stage = "clean",
        criticalColumns = cleanCfg.criticalColumns,
        reason = "critical_nulls"
      )
    }

    val cleanConfig = TransformClean.CleanConfig(
      criticalColumns = cleanCfg.criticalColumns,
      fillValues = cleanCfg.fillValues,
      castColumns = cleanCfg.castColumns.map { case (name, dt) => name -> parseDataType(dt) },
      stringColumns = cleanCfg.stringColumns,
      normalizeColNames = cleanCfg.normalizeColNames,
      dropFullDuplicates = cleanCfg.dropFullDuplicates,
      dropRowsWithNullsThreshold = cleanCfg.dropRowsWithNullsThreshold,
      verbose = cleanCfg.verbose
    )

    cleaned = TransformClean.clean(cleaned, cleanConfig)

    logger.info(s"Cleaning phase completed. Final row count: ${cleaned.count()}")
    cleaned
  }

  /**
   * Remove duplicate rows using the Window-based survival strategy.
   *
   * Before deduplication, rows with null values in the dedup key columns are
   * quarantined — a row without a business key cannot be meaningfully deduplicated
   * and would corrupt the Window partition.
   *
   * Returns the input DataFrame unchanged if `deduplicate.enabled = false` in config.
   *
   * @param df  Cleaned DataFrame from the clean stage.
   * @return    Deduplicated DataFrame with one surviving row per business key.
   */
  private def deduplicate(df: DataFrame): DataFrame = {
    logger.info("Deduplication phase")

    if (!appConfig.transform.deduplicate.enabled) return df

    val dedupCfg = appConfig.transform.deduplicate

    // implement the quarantine of duplicates if enabled before deduplication
    val quarantineHandler = new quality.QuarantineHandler(spark, logger)
    var dfToDedup = df

    if (appConfig.transform.deduplicate.enabled) {
      dfToDedup = quarantineHandler.quarantineAndClean(
        df = dfToDedup,
        runId = runId,
        stage = "dedup",
        criticalColumns = appConfig.transform.deduplicate.keyColumns,
        reason = "duplicates"
      )
    }

    val dedupConfig = TransformDeduplicate.DeduplicateConfig(
      keyColumns = dedupCfg.keyColumns,
      recencyColumn = dedupCfg.recencyColumn,
      sourcePriority = dedupCfg.sourcePriority,
      sourceTag = None,
      dropSourceCol = dedupCfg.dropSourceCol,
      verbose = dedupCfg.verbose
    )

    TransformDeduplicate.deduplicate(dfToDedup, dedupConfig)
  }

  /**
   * Apply all enabled join configurations in sequence.
   *
   * For each join in `transform.joins`, this method:
   *   1. Quarantines rows missing the join key columns.
   *   2. Loads the right-side DataFrame from the configured source type (flat/api/db).
   *   3. Normalises and flattens the right-side DataFrame.
   *   4. Executes the join via [[transform.TransformJoin.join]].
   *
   * Joins are applied left-to-right using `foldLeft`, so the output of join N
   * becomes the left side of join N+1. This enables chained enrichment steps
   * (e.g. join departments, then join cost centres).
   *
   * Returns the input DataFrame unchanged if no joins are enabled in config.
   *
   * @param df  Deduplicated DataFrame.
   * @return    Enriched DataFrame after all configured joins.
   */
  private def applyConfiguredJoins(df: DataFrame): DataFrame = {
    val activeJoins = appConfig.transform.joins.filter(_.enabled)
    if (activeJoins.isEmpty) return df

    // implement quarantine of rows with missing join keys if enabled before applying joins
    val quarantineHandler = new quality.QuarantineHandler(spark, logger)
    var dfToJoin = df
    activeJoins.foreach { j =>
      if (j.keyColumns.nonEmpty) {
        dfToJoin = quarantineHandler.quarantineAndClean(
          df = dfToJoin,
          runId = runId,
          stage = s"join_${j.name}",
          criticalColumns = j.keyColumns.map(TransformClean.toSnakeCase),
          reason = "missing_join_keys"
        )
      }
    }

    activeJoins.foldLeft(dfToJoin) { (leftDf, j) =>
      logger.info(s"Join phase - ${j.name}")
      val rightRaw = j.rightSourceType.trim.toLowerCase match {
        case "flat" =>
          ExtractFlatFiles.read(spark, j.rightPathOrQuery)
        case "api" =>
          val fromNamedApi = appConfig.extract.apis.find(_.name == j.rightDbRef).filter(_.enabled)
          fromNamedApi match {
            case Some(apiCfg) =>
              ExtractApi.read(
                spark = spark,
                url = apiCfg.url,
                method = apiCfg.method,
                params = apiCfg.params,
                headers = apiCfg.headers,
                rootField = apiCfg.rootField
              )
            case None =>
              ExtractApi.read(spark, j.rightPathOrQuery)
          }
        case "db" =>
          val dbCfg = appConfig.extract.databases.find(_.name == j.rightDbRef).getOrElse {
            throw new IllegalArgumentException(
              s"Join '${j.name}' references unknown database source '${j.rightDbRef}'."
            )
          }
          readFromDbConfig(dbCfg, j.rightPathOrQuery)
        case other =>
          throw new IllegalArgumentException(
            s"Join '${j.name}' has unsupported right-source-type '$other'. Supported: flat, api, db."
          )
      }

      val rightDf = normalizeAndMap(flattenStructs(rightRaw))
      val joinConfig = TransformJoin.JoinConfig(
        joinType = j.joinType,
        keyColumns = j.keyColumns,
        keyMappings = j.keyMappings,
        selectColumns = j.selectColumns,
        leftPrefix = j.leftPrefix,
        rightPrefix = j.rightPrefix,
        verbose = j.verbose
      )
      TransformJoin.join(leftDf, rightDf, joinConfig)
    }
  }

  /**
   * Apply GROUP BY aggregations as configured in `transform.aggregation`.
   *
   * Rows missing the aggregation group-by keys are quarantined before aggregation
   * to prevent null keys from creating a catch-all null group in the output.
   *
   * Returns the input DataFrame unchanged if `aggregation.enabled = false` in config.
   *
   * @param df  Joined/enriched DataFrame.
   * @return    Aggregated DataFrame, or the input DataFrame if aggregation is disabled.
   */
  private def aggregate(df: DataFrame): DataFrame = {
    logger.info("Aggregation phase")

    if (!appConfig.transform.aggregation.enabled) return df

    val aggregationCfg = appConfig.transform.aggregation

    // implement quarantine of rows with missing aggregation keys if enabled before applying aggregation
    val quarantineHandler = new quality.QuarantineHandler(spark, logger)
    var dfToAggregate = df
    if (aggregationCfg.enabled && aggregationCfg.groupBy.nonEmpty) {
      dfToAggregate = quarantineHandler.quarantineAndClean(
        df = dfToAggregate,
        runId = runId,
        stage = "aggregate",
        criticalColumns = aggregationCfg.groupBy.map(colName => TransformClean.toSnakeCase(colName)),
        reason = "missing_aggregation_keys"
      )
    }

    val aggregationConfig = TransformAggregate.AggregationConfig(
      enabled = aggregationCfg.enabled,
      groupByColumns = aggregationCfg.groupBy,
      metrics = aggregationCfg.metrics.map(metric =>
        TransformAggregate.AggregationMetric(metric.column, metric.function)
      ),
      suffixEnabled = aggregationCfg.suffixEnabled
    )

    TransformAggregate.aggregate(dfToAggregate, aggregationConfig)
  }

  /**
   * Write a DataFrame to the filesystem in the specified format.
   *
   * CSV output uses `coalesce(1)` to produce a single file for easy inspection.
   * Parquet output uses the default partition count (controlled by `shuffle.partitions`).
   *
   * Output path: `<output-base-path>/final/<timestamp>_final/<format>/`
   *
   * @param df      DataFrame to persist.
   * @param format  Output format: `"csv"` or `"parquet"` (default).
   */
  private def save(df: DataFrame, format: String = "parquet"): Unit = {
    val timestamp = System.currentTimeMillis()
    val path = s"$outputBase/final/${timestamp}_final/$format"

    format.toLowerCase match {
      case "csv" =>
        df.coalesce(1)
          .write.mode("overwrite")
          .option("header", "true")
          .csv(path)
      case _ =>
        df.write.mode("overwrite").parquet(path)
    }

    logger.info(s"Saved as $format -> $path (${df.count()} rows)")
  }

  /**
   * Map a config string to a Spark [[org.apache.spark.sql.types.DataType]].
   *
   * Used by the clean stage to resolve `cast-columns` config entries such as
   * `{ age = "integer", salary = "double" }` into actual Spark DataType instances.
   *
   * @param name  Type name string from config (case-insensitive).
   * @return      Corresponding Spark DataType.
   * @throws IllegalArgumentException for unrecognised type names.
   */
  private def parseDataType(name: String): DataType = {
    name.trim.toLowerCase match {
      case "string"     => StringType
      case "int" | "integer" => IntegerType
      case "long"       => LongType
      case "double"     => DoubleType
      case "float"      => FloatType
      case "date"       => DateType
      case "timestamp"  => TimestampType
      case "boolean"    => BooleanType
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported cast type '$other'. Supported: string,int,long,double,float,date,timestamp,boolean."
        )
    }
  }

  /**
   * Extract the lowercase file extension from a filename.
   * Returns `"unknown"` if no extension is present or the name ends with a dot.
   *
   * @param filename  Bare filename, e.g. `"employees_source1.csv"`.
   * @return          Lowercase extension without the dot, e.g. `"csv"`.
   */
  private def extractExtension(filename: String): String = {
    val idx = filename.lastIndexOf('.')
    if (idx < 0 || idx == filename.length - 1) "unknown"
    else filename.substring(idx + 1).toLowerCase
  }

  /**
   * List all readable items in a directory, filtering out hidden and temporary files.
   *
   * Files starting with `"."` (hidden) or `"_"` (Spark/Hadoop metadata like `_SUCCESS`)
   * are excluded. Parquet "files" that are actually directories are included.
   * Results are sorted alphabetically for deterministic processing order.
   *
   * @param folderPath  Path to the directory containing raw source files.
   * @return            Sorted array of [[java.io.File]] references, or empty if the
   *                    directory does not exist.
   */
  private def findItems(folderPath: String): Array[File] = {
    val dir = new File(folderPath)
    if (!dir.exists() || !dir.isDirectory) {
      println(s"❌ Folder not found: $folderPath")
      return Array.empty
    }

    dir.listFiles()
      .filter(f => f.isFile || (f.isDirectory && f.getName.endsWith(".parquet")))
      .filter(f => !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      .sortBy(_.getName)
  }

  /**
   * Run [[transform.QualityChecks.validate]] on the current DataFrame and log any issues.
   *
   * Called after the `clean` and `dedup` stages. Deduplication keys are excluded
   * from the check at the `"clean"` stage because deduplication hasn't run yet.
   *
   * If `fail-fast = true` and issues are found, throws [[IllegalStateException]]
   * to abort the pipeline. Otherwise, issues are logged as warnings and execution continues.
   *
   * @param df     DataFrame to validate.
   * @param stage  Stage label for the log message, e.g. `"clean"` or `"dedup"`.
   */
  private def runQualityChecks(df: DataFrame, stage: String): Unit = {
    val dedupKeys = if (stage == "clean") Seq.empty else appConfig.quality.deduplicationKeys.map(TransformClean.toSnakeCase)
    val cfg = QualityChecks.QualityConfig(
      enabled = appConfig.quality.enabled,
      criticalColumns = appConfig.quality.criticalColumns.map(TransformClean.toSnakeCase),
      maxNullRatioPerRow = appConfig.quality.maxNullRatioPerRow,
      deduplicationKeys = dedupKeys
    )
    val issues = QualityChecks.validate(df, cfg)
    if (issues.nonEmpty) {
      logger.warn(s"Quality checks found issues at stage '$stage': ${issues.mkString(", ")}")
      println(s"Quality checks details: ${issues.mkString(", ")}")
      if (appConfig.orchestration.failFast) {
        throw new IllegalStateException(s"Quality check failed at stage '$stage'")
      }
    }
  }
}
