package config

import com.typesafe.config.{Config, ConfigFactory}
import scala.jdk.CollectionConverters._
import java.io.File

object PipelineConfig {

  case class AppConfig(
    extract: ExtractConfig,
    transform: TransformConfig,
    load: LoadConfig
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

  case class ApiSourceConfig(
    name: String,
    enabled: Boolean,
    url: String,
    method: String,
    params: Map[String, String],
    headers: Map[String, String],
    rootField: String,
    sourceTag: String
  )

  case class DatabaseSourceConfig(
    name: String,
    enabled: Boolean,
    dbType: String,
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    sqliteFilePath: String,
    tableOrQuery: String,
    sourceTag: String,
    partitionColumn: Option[String],
    lowerBound: Option[Long],
    upperBound: Option[Long],
    numPartitions: Int
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

  case class JoinConfig(
    enabled: Boolean,
    name: String,
    rightSourceType: String, // "flat" | "api" | "db"
    rightPathOrQuery: String,
    rightDbRef: String,
    joinType: String,
    keyColumns: Seq[String],
    keyMappings: Seq[(String, String)],
    selectColumns: Seq[String],
    leftPrefix: String,
    rightPrefix: String,
    verbose: Boolean
  )

  case class LoadConfig(
    enabledFormats: Seq[String],
    outputBasePath: String
  )

  def load(configPath: String = "application.conf"): AppConfig = {
    val rootConfig =
      if (new File(configPath).exists()) {
        ConfigFactory.parseFile(new File(configPath)).withFallback(ConfigFactory.load()).resolve()
      } else {
        ConfigFactory.load()
      }
    val root = rootConfig.getConfig("etl")

    AppConfig(
      extract = parseExtract(root.getConfig("extract")),
      transform = parseTransform(root.getConfig("transform")),
      load = parseLoad(root.getConfig("load"))
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
      outputBasePath = c.getString("output-base-path")
    )

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
