package config

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters._
import java.io.File

/**
 * PipelineConfig — central configuration model for the ETL pipeline.
 *
 * Reads HOCON configuration from `application.conf` (or a file path supplied
 * at runtime) using the Typesafe Config library and maps every section to a
 * strongly-typed Scala case class. This guarantees that configuration errors
 * are caught at pipeline startup rather than inside a running Spark stage.
 *
 * Configuration is structured under the `etl` root key:
 * {{{
 *   etl {
 *     spark { ... }        // SparkSession settings
 *     extract { ... }      // Source system declarations
 *     transform { ... }    // Cleaning, dedup, join, aggregation settings
 *     quality { ... }      // Post-stage quality gate thresholds
 *     warehouse { ... }    // Star-schema table definitions
 *     load { ... }         // Output formats, warehouse target, upsert settings
 *     orchestration { ... }// Stage list, fail-fast flag
 *     logging { ... }      // Log level, metrics toggle
 *     audit { ... }        // Audit log output path
 *     quarantine { ... }   // Quarantine output base path
 *   }
 * }}}
 *
 * All `parse*` helpers use safe accessors (`getStringOrElse`, `getIntOrElse`, etc.)
 * that return a default instead of throwing when a key is absent. This lets the
 * pipeline start with a minimal config and rely on sensible defaults everywhere.
 */
object PipelineConfig {

  /**
   * Root configuration object — aggregates all pipeline sections.
   * Constructed once at startup by [[load]] and passed to [[Main.EtlPipeline]].
   */
  case class AppConfig(
    spark:         SparkConfig,
    extract:       ExtractConfig,
    transform:     TransformConfig,
    quality:       QualityConfig,
    warehouse:     WarehouseConfig,
    load:          LoadConfig,
    orchestration: OrchestrationConfig,
    logging:       LoggingConfig,
    audit:         AuditConfig,
    quarantine:    QuarantineConfig,
  )

  /**
   * Spark session settings.
   * @param appName          Application name shown in the Spark UI.
   * @param master           Spark master URL. `"local[*]"` for local mode (all cores).
   *                         Change to `"spark://host:7077"` or `"yarn"` for a cluster.
   * @param shufflePartitions Number of partitions after a shuffle operation (joins, groupBy).
   *                         Default 8 is suitable for local mode; increase for cluster runs.
   */
  case class SparkConfig(
    appName:           String,
    master:            String,
    shufflePartitions: Int
  )

  /**
   * Controls where rejected (quarantined) rows are written.
   * @param enabled   If false, [[quality.QuarantineHandler]] writes bad rows but
   *                  the flag is not yet propagated to suppress writing (extend if needed).
   * @param basePath  Root directory for quarantine output. Subdirectories are created
   *                  per run-id and stage automatically.
   */
  case class QuarantineConfig(
    enabled:  Boolean,
    basePath: String
  )

  /**
   * Audit log configuration.
   * @param enabled     Reserved for future use — the [[audit.AuditLogger]] always writes
   *                    when instantiated. Set false to suppress in future versions.
   * @param outputPath  Directory for the audit log file (currently unused; the log path
   *                    is set directly in [[Main.EtlPipeline]]).
   */
  case class AuditConfig(
    enabled:    Boolean,
    outputPath: String
  )

  /**
   * Post-stage quality gate configuration.
   * @param enabled              If false, [[transform.QualityChecks.validate]] returns
   *                             an empty list — all checks are skipped.
   * @param criticalColumns      Columns that must be non-null after cleaning.
   * @param maxNullRatioPerRow   Rows with this proportion of nulls (0.0–1.0) are flagged.
   *                             Example: 0.7 flags rows with more than 70% null columns.
   * @param deduplicationKeys    Business-key columns checked for remaining duplicates
   *                             after the dedup stage.
   */
  case class QualityConfig(
    enabled:            Boolean,
    criticalColumns:    Seq[String],
    maxNullRatioPerRow: Double,
    deduplicationKeys:  Seq[String]
  )

  /**
   * Star-schema warehouse configuration.
   * @param enabled     Master toggle — when false, [[load.StarSchemaBuilder]] still builds
   *                    the DataFrames but [[load.WarehouseLoader]] skips the JDBC write.
   * @param dimensions  Ordered list of dimension table descriptors.
   * @param fact        Fact table descriptor.
   */
  case class WarehouseConfig(
    enabled:    Boolean,
    dimensions: Seq[WarehouseTableConfig],
    fact:       WarehouseTableConfig
  )

  /**
   * Descriptor for a single warehouse table (dimension or fact).
   * @param name          Target table name in the warehouse database, e.g. `"dim_employee"`.
   * @param surrogateKey  Name of the surrogate key column generated by dense_rank().
   * @param businessKeys  Natural/business keys used for upsert conflict detection.
   */
  case class WarehouseTableConfig(
    name:         String,
    surrogateKey: String,
    businessKeys: Seq[String]
  )

  /**
   * Pipeline execution control.
   * @param stages                        Ordered list of stage names to execute.
   *                                      An empty list enables all stages in default order.
   *                                      Example: `["extract", "clean", "load"]` skips dedup/join.
   * @param failFast                      If true, any stage exception aborts the pipeline.
   *                                      If false, failed stages return `None` and execution continues.
   * @param skipNonCriticalSourceFailures If true, a failing API or DB source is logged as a
   *                                      warning and skipped rather than crashing the extract stage.
   */
  case class OrchestrationConfig(
    stages:                       Seq[String],
    failFast:                     Boolean,
    skipNonCriticalSourceFailures: Boolean
  )

  /**
   * Logging settings.
   * @param level          Root log level for the SLF4J/Log4j 2 configuration.
   *                       Spark's own log level is set separately via
   *                       `spark.sparkContext.setLogLevel("ERROR")` in [[Main]].
   * @param metricsEnabled If true, [[logging.PipelineLogger]] records per-stage
   *                       wall-clock durations and emits them in `stepEndLog` messages.
   */
  case class LoggingConfig(
    level:          String,
    metricsEnabled: Boolean
  )

  case class ExtractConfig(
    flatFiles: FlatFilesConfig,
    apis: Seq[ApiSourceConfig],
    databases: Seq[DatabaseSourceConfig]
  )

  case class FlatFilesConfig(
    enabled: Boolean,
    path: String,
    excludeNameContains: Seq[String]
  )

  /**
   * Configuration for a single REST API source.
   * @param name       Human-readable identifier used in logs, e.g. `"jsonplaceholder-users"`.
   * @param enabled    Toggle to include/exclude this source without removing the config block.
   * @param url        Full endpoint URL.
   * @param method     HTTP method: `"GET"` or `"POST"`. Defaults to `"GET"`.
   * @param params     Query string parameters, e.g. `{ limit = "100" }`.
   * @param headers    HTTP headers, e.g. `{ Authorization = "Bearer ..." }`.
   * @param rootField  Top-level JSON key whose value is the records array.
   *                   Leave empty if the response is a flat JSON array.
   * @param sourceTag  Label written to the `__source` column for deduplication priority.
   */
  case class ApiSourceConfig(
    name:      String,
    enabled:   Boolean,
    url:       String,
    method:    String,
    params:    Map[String, String],
    headers:   Map[String, String],
    rootField: String,
    sourceTag: String
  )

  /**
   * Configuration for a single JDBC database source.
   * @param name             Identifier used in logs and join `right-db-ref` lookups.
   * @param enabled          Toggle — set false when the DB container is not running.
   * @param dbType           `"postgres"` | `"mysql"` | `"sqlite"`.
   * @param host             Database host. Ignored for SQLite.
   * @param port             Database port. Defaults to driver default when 0.
   * @param database         Database/schema name. Ignored for SQLite.
   * @param user             DB username. Ignored for SQLite.
   * @param password         DB password. Ignored for SQLite.
   * @param sqliteFilePath   Path to the `.db` file. Used only when `dbType = "sqlite"`.
   * @param tableOrQuery     Plain table name or parenthesised SQL subquery with alias.
   * @param sourceTag        Label for the `__source` deduplication column.
   * @param partitionColumn  Optional numeric column for parallel partitioned reads.
   * @param lowerBound       Minimum value of `partitionColumn` for range splitting.
   * @param upperBound       Maximum value of `partitionColumn` for range splitting.
   * @param numPartitions    Number of parallel Spark tasks for partitioned reads. Default 4.
   */
  case class DatabaseSourceConfig(
    name:            String,
    enabled:         Boolean,
    dbType:          String,
    host:            String,
    port:            Int,
    database:        String,
    user:            String,
    password:        String,
    sqliteFilePath:  String,
    tableOrQuery:    String,
    sourceTag:       String,
    partitionColumn: Option[String],
    lowerBound:      Option[Long],
    upperBound:      Option[Long],
    numPartitions:   Int
  )

  case class TransformConfig(
    normalizeColumns: Boolean,
    columnMapping: Map[String, String],
    clean: CleanConfig,
    deduplicate: DeduplicateConfig,
    joins: Seq[JoinConfig],
    aggregation: AggregationConfig
  )

  case class AggregationConfig(
    enabled: Boolean,
    groupBy: Seq[String],
    metrics: Seq[AggregationMetricConfig],
    suffixEnabled: Boolean
  )

  case class AggregationMetricConfig(
    column: String,
    function: String
  )

  case class CleanConfig(
    enabled: Boolean,
    criticalColumns: Seq[String],
    fillValues: Map[String, String],
    stringColumns: Seq[String],
    castColumns: Map[String, String],
    dropRowsWithNullsThreshold: Double,
    dropFullDuplicates: Boolean,
    normalizeColNames: Boolean,
    verbose: Boolean
  )

  case class DeduplicateConfig(
    enabled: Boolean,
    keyColumns: Seq[String],
    recencyColumn: Option[String],
    sourcePriority: Map[String, Int],
    dropSourceCol: Boolean,
    verbose: Boolean
  )

  /**
   * Configuration for a single join step in the transform chain.
   * @param enabled          Toggle to enable/disable this join without removing the block.
   * @param name             Descriptive name used in logs, e.g. `"employees-with-departments"`.
   * @param rightSourceType  Type of the right-side data source: `"flat"` | `"api"` | `"db"`.
   * @param rightPathOrQuery File path, API URL, or SQL table/query for the right side.
   * @param rightDbRef       For `"db"` type: the `name` of a database source in `extract.databases`.
   *                         For `"api"` type: optionally the `name` of an API source.
   * @param joinType         Spark join type: `"inner"` | `"left"` | `"right"` | `"full"`.
   * @param keyColumns       Join columns with identical names on both sides.
   *                         Use `keyMappings` instead when column names differ.
   * @param keyMappings      Explicit left→right column name pairs for asymmetric joins.
   *                         Example: `[{ left = "department", right = "dept_id" }]`.
   * @param selectColumns    Columns to keep in the output. Empty = keep all.
   * @param leftPrefix       Prefix added to all non-key left-side columns before joining.
   * @param rightPrefix      Prefix added to all non-key right-side columns before joining.
   * @param verbose          If true, print join row counts before and after.
   */
  case class JoinConfig(
    enabled:          Boolean,
    name:             String,
    rightSourceType:  String,   // "flat" | "api" | "db"
    rightPathOrQuery: String,
    rightDbRef:       String,
    joinType:         String,
    keyColumns:       Seq[String],
    keyMappings:      Seq[(String, String)],
    selectColumns:    Seq[String],
    leftPrefix:       String,
    rightPrefix:      String,
    verbose:          Boolean
  )

  case class LoadConfig(
    enabledFormats: Seq[String],
    outputBasePath: String,
    warehouseTarget: WarehouseTargetConfig,
    upsert: UpsertConfig
  )

  case class WarehouseTargetConfig(
    enabled: Boolean,
    dbType: String,
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    sqliteFilePath: String,
    schema: String
  )

  /**
   * Upsert (INSERT ... ON CONFLICT) behaviour settings.
   * @param enabled       Master toggle for the staging-table upsert pattern.
   * @param stagingPrefix Prefix prepended to the target table name to form the staging
   *                      table name. Default: `"stg_"` → `stg_dim_employee`.
   * @param batchSize     Rows per JDBC batch insert into the staging table. Default: 500.
   *                      Increase for large tables; decrease if the target DB has memory limits.
   */
  case class UpsertConfig(
    enabled:       Boolean,
    stagingPrefix: String,
    batchSize:     Int
  )

  /**
   * Load and parse the full pipeline configuration.
   *
   * Resolution order:
   *   1. If `configPath` points to an existing file, parse it first.
   *   2. Fall back to the classpath `application.conf` (standard Typesafe Config behaviour).
   *   3. Resolve variable substitutions (`${?VAR}` patterns).
   *
   * All section parsers use safe accessors so the pipeline starts successfully
   * even when optional sections (quality, warehouse, orchestration) are absent
   * from the config file — sensible defaults are applied.
   *
   * @param configPath  Path to the HOCON config file. Defaults to `"application.conf"`
   *                    (resolved relative to the working directory at runtime).
   * @return            Fully parsed [[AppConfig]] ready for use by [[Main.EtlPipeline]].
   */
  def load(configPath: String = "application.conf"): AppConfig = {
    val rootConfig =
      if (new File(configPath).exists()) {
        ConfigFactory.parseFile(new File(configPath)).withFallback(ConfigFactory.load()).resolve()
      } else {
        ConfigFactory.load()
      }
    val root = rootConfig.getConfig("etl")

    AppConfig(
      spark = parseSpark(root),
      extract = parseExtract(root.getConfig("extract")),
      transform = parseTransform(root.getConfig("transform")),
      quality = parseQuality(getConfigOrElse(root, "quality", ConfigFactory.parseString(""))),
      warehouse = parseWarehouse(getConfigOrElse(root, "warehouse", ConfigFactory.parseString(""))),
      load = parseLoad(root.getConfig("load")),
      orchestration = parseOrchestration(getConfigOrElse(root, "orchestration", ConfigFactory.parseString(""))),
      logging = parseLogging(getConfigOrElse(root, "logging", ConfigFactory.parseString(""))),
      audit = parseAudit(root.getConfig("audit")),
      quarantine = parseQuarantine(root.getConfig("quarantine")),
    )
  }

  private def parseSpark(c: Config): SparkConfig = {
    val spark = getConfigOrElse(c, "spark", ConfigFactory.parseString(""))
    SparkConfig(
      appName = getStringOrElse(spark, "app-name", "Distributed ETL Pipeline"),
      master = getStringOrElse(spark, "master", "local[*]"),
      shufflePartitions = getIntOrElse(spark, "shuffle-partitions", 8)
    )
  }

  private def parseQuarantine(c: Config): QuarantineConfig = {
    QuarantineConfig(
      enabled = getBooleanOrElse(c, "enabled", false),
      basePath = getStringOrElse(c, "base-path", "output/quarantine")
    )
  }

  private def parseExtract(c: Config): ExtractConfig = {
    val flat = c.getConfig("flat-files")
    ExtractConfig(
      flatFiles = FlatFilesConfig(
        enabled = flat.getBoolean("enabled"),
        path = flat.getString("path"),
        excludeNameContains = getStringSeq(flat, "exclude-name-contains")
      ),
      apis = getConfigSeq(c, "apis").map(parseApiSource),
      databases = getConfigSeq(c, "databases").map(parseDbSource)
    )
  }

  private def parseApiSource(c: Config): ApiSourceConfig =
    ApiSourceConfig(
      name = c.getString("name"),
      enabled = c.getBoolean("enabled"),
      url = c.getString("url"),
      method = getStringOrElse(c, "method", "GET"),
      params = getStringMap(c, "params"),
      headers = getStringMap(c, "headers"),
      rootField = getStringOrElse(c, "root-field", ""),
      sourceTag = getStringOrElse(c, "source-tag", c.getString("name"))
    )

  private def parseDbSource(c: Config): DatabaseSourceConfig =
    DatabaseSourceConfig(
      name = c.getString("name"),
      enabled = c.getBoolean("enabled"),
      dbType = c.getString("db-type"),
      host = getStringOrElse(c, "host", "localhost"),
      port = getIntOrElse(c, "port", 0),
      database = getStringOrElse(c, "database", ""),
      user = getStringOrElse(c, "user", ""),
      password = getStringOrElse(c, "password", ""),
      sqliteFilePath = getStringOrElse(c, "sqlite-file-path", ""),
      tableOrQuery = c.getString("table-or-query"),
      sourceTag = getStringOrElse(c, "source-tag", c.getString("name")),
      partitionColumn = getOptString(c, "partition-column"),
      lowerBound = getOptLong(c, "lower-bound"),
      upperBound = getOptLong(c, "upper-bound"),
      numPartitions = getIntOrElse(c, "num-partitions", 4)
    )

  private def parseTransform(c: Config): TransformConfig = {
    val clean = c.getConfig("clean")
    val dedup = c.getConfig("deduplicate")
    TransformConfig(
      normalizeColumns = c.getBoolean("normalize-columns"),
      columnMapping = getStringMap(c, "column-mapping"),
      clean = CleanConfig(
        enabled = clean.getBoolean("enabled"),
        criticalColumns = getStringSeq(clean, "critical-columns"),
        fillValues = getStringMap(clean, "fill-values"),
        stringColumns = getStringSeq(clean, "string-columns"),
        castColumns = getStringMap(clean, "cast-columns"),
        dropRowsWithNullsThreshold = getDoubleOrElse(clean, "drop-rows-with-nulls-threshold", 0.0),
        dropFullDuplicates = getBooleanOrElse(clean, "drop-full-duplicates", true),
        normalizeColNames = getBooleanOrElse(clean, "normalize-col-names", true),
        verbose = getBooleanOrElse(clean, "verbose", true)
      ),
      deduplicate = DeduplicateConfig(
        enabled = dedup.getBoolean("enabled"),
        keyColumns = getStringSeq(dedup, "key-columns"),
        recencyColumn = getOptString(dedup, "recency-column"),
        sourcePriority = getIntMap(dedup, "source-priority"),
        dropSourceCol = getBooleanOrElse(dedup, "drop-source-col", true),
        verbose = getBooleanOrElse(dedup, "verbose", true)
      ),
      joins = getConfigSeq(c, "joins").map(parseJoinConfig),
      aggregation = parseAggregation(c.getConfig("aggregation"))
    )
  }

  private def parseAggregation(c: Config): AggregationConfig =
    AggregationConfig(
      enabled = c.getBoolean("enabled"),
      groupBy = getStringSeq(c, "group-by"),
      metrics = getConfigSeq(c, "metrics").map { metricCfg =>
        AggregationMetricConfig(
          column = metricCfg.getString("column"),
          function = metricCfg.getString("function")
        )
      },
      suffixEnabled = getBooleanOrElse(c, "suffix-enabled", true)
    )

  private def parseJoinConfig(c: Config): JoinConfig =
    JoinConfig(
      enabled = c.getBoolean("enabled"),
      name = c.getString("name"),
      rightSourceType = c.getString("right-source-type"),
      rightPathOrQuery = getStringOrElse(c, "right-path-or-query", ""),
      rightDbRef = getStringOrElse(c, "right-db-ref", ""),
      joinType = c.getString("join-type"),
      keyColumns = getStringSeq(c, "key-columns"),
      keyMappings = parseKeyMappings(c),
      selectColumns = getStringSeq(c, "select-columns"),
      leftPrefix = getStringOrElse(c, "left-prefix", ""),
      rightPrefix = getStringOrElse(c, "right-prefix", ""),
      verbose = getBooleanOrElse(c, "verbose", true)
    )

  private def parseKeyMappings(c: Config): Seq[(String, String)] = {
    getConfigSeq(c, "key-mappings").zipWithIndex.map { case (m, idx) =>
      try {
        val left = m.getString("left").trim
        val right = m.getString("right").trim
        if (left.isEmpty || right.isEmpty) {
          throw new IllegalArgumentException("left/right values must be non-empty.")
        }
        left -> right
      } catch {
        case e: Exception =>
          val joinName = getStringOrElse(c, "name", "<unknown-join>")
          throw new IllegalArgumentException(
            s"Invalid key-mappings entry in join '$joinName' at index $idx. " +
            s"Each entry must contain non-empty 'left' and 'right' fields.",
            e
          )
      }
    }
  }

  private def parseLoad(c: Config): LoadConfig =
    LoadConfig(
      enabledFormats = getStringSeq(c, "enabled-formats"),
      outputBasePath = c.getString("output-base-path"),
      warehouseTarget = parseWarehouseTarget(getConfigOrElse(c, "warehouse-target", ConfigFactory.parseString(""))),
      upsert = parseUpsert(getConfigOrElse(c, "upsert", ConfigFactory.parseString("")))
    )

  private def parseWarehouseTarget(c: Config): WarehouseTargetConfig =
    WarehouseTargetConfig(
      enabled = getBooleanOrElse(c, "enabled", false),
      dbType = getStringOrElse(c, "db-type", "sqlite"),
      host = getStringOrElse(c, "host", "localhost"),
      port = getIntOrElse(c, "port", 0),
      database = getStringOrElse(c, "database", ""),
      user = getStringOrElse(c, "user", ""),
      password = getStringOrElse(c, "password", ""),
      sqliteFilePath = getStringOrElse(c, "sqlite-file-path", ""),
      schema = getStringOrElse(c, "schema", "public")
    )

  private def parseUpsert(c: Config): UpsertConfig =
    UpsertConfig(
      enabled = getBooleanOrElse(c, "enabled", false),
      stagingPrefix = getStringOrElse(c, "staging-prefix", "stg_"),
      batchSize = getIntOrElse(c, "batch-size", 500)
    )

  private def parseQuality(c: Config): QualityConfig =
    QualityConfig(
      enabled = getBooleanOrElse(c, "enabled", false),
      criticalColumns = getStringSeq(c, "critical-columns"),
      maxNullRatioPerRow = getDoubleOrElse(c, "max-null-ratio-per-row", 1.0),
      deduplicationKeys = getStringSeq(c, "deduplication-keys")
    )

  private def parseWarehouse(c: Config): WarehouseConfig = {
    val defaultFact = WarehouseTableConfig(
      name = "fact_employee_metrics",
      surrogateKey = "fact_id",
      businessKeys = Seq("employee_sk", "department_sk", "date_sk")
    )
    WarehouseConfig(
      enabled = getBooleanOrElse(c, "enabled", false),
      dimensions = getConfigSeq(c, "dimensions").map(parseWarehouseTable),
      fact = if (c.hasPath("fact")) parseWarehouseTable(c.getConfig("fact")) else defaultFact
    )
  }

  private def parseWarehouseTable(c: Config): WarehouseTableConfig =
    WarehouseTableConfig(
      name = c.getString("name"),
      surrogateKey = getStringOrElse(c, "surrogate-key", "id"),
      businessKeys = getStringSeq(c, "business-keys")
    )

  private def parseOrchestration(c: Config): OrchestrationConfig =
    OrchestrationConfig(
      stages = getStringSeq(c, "stages"),
      failFast = getBooleanOrElse(c, "fail-fast", true),
      skipNonCriticalSourceFailures = getBooleanOrElse(c, "skip-non-critical-source-failures", true)
    )

  private def parseLogging(c: Config): LoggingConfig =
    LoggingConfig(
      level = getStringOrElse(c, "level", "INFO"),
      metricsEnabled = getBooleanOrElse(c, "metrics-enabled", true)
    )

  private def parseAudit(c: Config): AuditConfig = {
    AuditConfig(
      enabled = getBooleanOrElse(c, "enabled", true),
      outputPath = getStringOrElse(c, "output-path", "output/audit")
    )
  }

  private def getStringSeq(c: Config, key: String): Seq[String] =
    if (c.hasPath(key)) c.getStringList(key).asScala.toSeq else Seq.empty

  private def getStringMap(c: Config, key: String): Map[String, String] =
    if (!c.hasPath(key)) Map.empty
    else {
      val sub = c.getConfig(key)
      sub.entrySet().asScala.map { e =>
        e.getKey -> sub.getString(e.getKey)
      }.toMap
    }

  private def getIntMap(c: Config, key: String): Map[String, Int] =
    if (!c.hasPath(key)) Map.empty
    else {
      val sub = c.getConfig(key)
      sub.entrySet().asScala.map { e =>
        e.getKey -> sub.getInt(e.getKey)
      }.toMap
    }

  private def getConfigSeq(c: Config, key: String): Seq[Config] =
    if (!c.hasPath(key)) Seq.empty else c.getConfigList(key).asScala.toSeq

  private def getStringOrElse(c: Config, key: String, default: String): String =
    if (c.hasPath(key)) c.getString(key) else default

  private def getConfigOrElse(c: Config, key: String, default: Config): Config =
    if (c.hasPath(key)) c.getConfig(key) else default

  private def getOptString(c: Config, key: String): Option[String] =
    if (c.hasPath(key)) Option(c.getString(key)).filter(_.nonEmpty) else None

  private def getIntOrElse(c: Config, key: String, default: Int): Int =
    if (c.hasPath(key)) c.getInt(key) else default

  private def getOptLong(c: Config, key: String): Option[Long] =
    if (c.hasPath(key)) Some(c.getLong(key)) else None

  private def getDoubleOrElse(c: Config, key: String, default: Double): Double =
    if (c.hasPath(key)) c.getDouble(key) else default

  private def getBooleanOrElse(c: Config, key: String, default: Boolean): Boolean =
    if (c.hasPath(key)) c.getBoolean(key) else default
}
