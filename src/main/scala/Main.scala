import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import java.io.File

import config.PipelineConfig
import config.PipelineConfig.{AppConfig, DatabaseSourceConfig}
import extract.{ExtractApi, ExtractDatabase, ExtractFlatFiles}
import load.{StarSchemaBuilder, WarehouseLoader}
import logging.PipelineLogger
import orchestration.PipelineOrchestrator
import transform.QualityChecks
import transform.{TransformClean, TransformDeduplicate, TransformJoin, TransformAggregate}

object Main {

  def main(args: Array[String]): Unit = {
    // Load config first because SparkSession settings (app name/master/partitions) come from it.
    val config = PipelineConfig.load()

    val spark = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions.toString)
      .getOrCreate()

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

class EtlPipeline(spark: SparkSession, appConfig: AppConfig) {

  private val outputBase = appConfig.load.outputBasePath
  private val logger = new PipelineLogger(appConfig.logging.level, appConfig.logging.metricsEnabled)
  private val orchestrator = new PipelineOrchestrator(
    appConfig.orchestration.stages,
    appConfig.orchestration.failFast,
    logger
  )

  def run(): Unit = {
    logger.info("=" * 90)
    logger.info("Starting Distributed ETL Pipeline")
    logger.info("=" * 90)

    val rawDF = orchestrator.runStage("extract") { extractAllSources() }.getOrElse(spark.emptyDataFrame)

    if (rawDF.isEmpty) {
      logger.warn("No data extracted. Exiting.")
      return
    }

    val cleanedDF = orchestrator.runStage("clean") { clean(rawDF) }.getOrElse(rawDF)
    runQualityChecks(cleanedDF, "clean")

    val deduplicatedDF = orchestrator.runStage("dedup") { deduplicate(cleanedDF) }.getOrElse(cleanedDF)
    runQualityChecks(deduplicatedDF, "dedup")

    val enrichedDF = orchestrator.runStage("join") { applyConfiguredJoins(deduplicatedDF) }.getOrElse(deduplicatedDF)

    val aggregatedDF = orchestrator.runStage("aggregate") { aggregate(enrichedDF) }.getOrElse(enrichedDF)

    val starSchema = orchestrator.runStage("build_star") {
      StarSchemaBuilder.build(spark, enrichedDF, appConfig.warehouse, logger.warn)
    }

    orchestrator.runStage("load") {
      appConfig.load.enabledFormats.foreach(fmt => save(aggregatedDF, fmt))
      starSchema.foreach { star =>
        WarehouseLoader.loadStarSchema(star, appConfig.warehouse, appConfig.load.warehouseTarget, appConfig.load.upsert)
      }
    }

    logger.info("Full ETL Pipeline completed successfully.")
  }

  private def extractAllSources(): DataFrame = {
    println("\n📥 Extraction Phase - Loading all sources")

    val extracted = extractFlatFiles() ++ extractApis() ++ extractDatabases()
    extracted.reduceOption(_.unionByName(_, allowMissingColumns = true)).getOrElse(spark.emptyDataFrame)
  }

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

  private def clean(df: DataFrame): DataFrame = {
    logger.info("Cleaning phase")

    if (!appConfig.transform.clean.enabled) return df

    val cleanCfg = appConfig.transform.clean
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

    TransformClean.clean(df, cleanConfig)
  }

  private def deduplicate(df: DataFrame): DataFrame = {
    logger.info("Deduplication phase")

    if (!appConfig.transform.deduplicate.enabled) return df

    val dedupCfg = appConfig.transform.deduplicate
    val dedupConfig = TransformDeduplicate.DeduplicateConfig(
      keyColumns = dedupCfg.keyColumns,
      recencyColumn = dedupCfg.recencyColumn,
      sourcePriority = dedupCfg.sourcePriority,
      sourceTag = None,
      dropSourceCol = dedupCfg.dropSourceCol,
      verbose = dedupCfg.verbose
    )

    TransformDeduplicate.deduplicate(df, dedupConfig)
  }

  private def applyConfiguredJoins(df: DataFrame): DataFrame = {
    val activeJoins = appConfig.transform.joins.filter(_.enabled)
    if (activeJoins.isEmpty) return df

    activeJoins.foldLeft(df) { (leftDf, j) =>
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

  private def aggregate(df: DataFrame): DataFrame = {
    logger.info("Aggregation phase")

    if (!appConfig.transform.aggregation.enabled) return df

    val aggregationCfg = appConfig.transform.aggregation
    val aggregationConfig = TransformAggregate.AggregationConfig(
      enabled = aggregationCfg.enabled,
      groupByColumns = aggregationCfg.groupBy,
      metrics = aggregationCfg.metrics.map(metric =>
        TransformAggregate.AggregationMetric(metric.column, metric.function)
      ),
      suffixEnabled = aggregationCfg.suffixEnabled
    )

    TransformAggregate.aggregate(df, aggregationConfig)
  }

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

  private def extractExtension(filename: String): String = {
    val idx = filename.lastIndexOf('.')
    if (idx < 0 || idx == filename.length - 1) "unknown"
    else filename.substring(idx + 1).toLowerCase
  }

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

  private def runQualityChecks(df: DataFrame, stage: String): Unit = {
    val cfg = QualityChecks.QualityConfig(
      enabled = appConfig.quality.enabled,
      criticalColumns = appConfig.quality.criticalColumns,
      maxNullRatioPerRow = appConfig.quality.maxNullRatioPerRow,
      deduplicationKeys = appConfig.quality.deduplicationKeys
    )
    val issues = QualityChecks.validate(df, cfg)
    if (issues.nonEmpty) {
      logger.warn(s"Quality checks found issues at stage '$stage': ${issues.mkString(", ")}")
      if (appConfig.orchestration.failFast) {
        throw new IllegalStateException(s"Quality check failed at stage '$stage'")
      }
    }
  }
}
