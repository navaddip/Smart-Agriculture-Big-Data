package smartagri.config

/**
 * Centralized, structural configuration for the project.
 *
 * Scope of THIS task only: paths, grid sizing, and the connection-info
 * placeholders that the future Kafka producer/Structured Streaming job
 * (not implemented yet) will need. This deliberately does NOT include
 * Kalman, adaptive-reliability, ML, or conformal-prediction parameters --
 * those belong inside their own modules once those modules exist (see the
 * Phase-3 Task 1 "DO NOT DO" list). Adding speculative tuning constants
 * for an algorithm that doesn't exist yet would just be a second place for
 * them to drift out of sync later.
 *
 * Every default below matches a value the existing legacy Scala code
 * already hard-codes (`HdfsUtils.HDFS`, `SoilGridsIngestion`/
 * `Era5Ingestion`/`SentinelIngestion`'s `RAW`/`OUT` constants,
 * `SentinelIngestion.GRID`, and `kafka-compose.yml`'s advertised
 * listener). Adopting `AppConfig` is therefore backward compatible --
 * nothing about the running system changes unless an environment
 * variable explicitly overrides it. No new dependency (e.g. Typesafe
 * Config) was added for this: plain environment-variable overrides are
 * sufficient for this task's scope and keep the dependency list
 * unchanged, per the "do not install unnecessary dependencies" rule.
 */
final case class HdfsPaths(
    /** e.g. "hdfs://namenode:9000/user/smart_agri" -- matches HdfsUtils.HDFS + "/user/smart_agri" today. */
    base: String,
    /** Raw, immutable source downloads. Per-modality subfolders (raw/sentinel2, raw/era5, raw/soilgrids)
      * are still appended by each ingestion module, unchanged, in this task. */
    raw: String,
    /** Canonical batch tables (NDVI, ERA5, SoilGrids, and their Scala-produced counterparts). */
    processed: String,
    /** Reserved for the future Structured Streaming checkpoint + materialized IoT state (not used yet). */
    streaming: String,
    /** Kalman states, labeled data, ML/conformal/irrigation outputs. */
    results: String
)

final case class KafkaSettings(
    /** Matches kafka-compose.yml's `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`. */
    bootstrapServers: String,
    /** Reserved topic name for the future IoT stream (reconciliation blueprint §5/§13). No producer/consumer exists yet. */
    iotTopic: String
)

final case class GridSettings(
    /** Cells per tile edge, e.g. 100 => a 100x100 grid. Matches `SentinelIngestion.GRID = 100` today. */
    cellsPerTileEdge: Int
)

final case class AppConfig(
    hdfs: HdfsPaths,
    kafka: KafkaSettings,
    grid: GridSettings
)

object AppConfig {

  private def env(name: String, default: String): String =
    sys.env.getOrElse(name, default)

  private def envInt(name: String, default: Int): Int =
    sys.env.get(name).map(_.toInt).getOrElse(default)

  /** Default configuration -- every value matches an existing hard-coded constant in the legacy modules. */
  def default: AppConfig = {
    val hdfsBase = env("SMART_AGRI_HDFS_BASE", "hdfs://namenode:9000/user/smart_agri")
    AppConfig(
      hdfs = HdfsPaths(
        base = hdfsBase,
        raw = env("SMART_AGRI_HDFS_RAW", s"$hdfsBase/raw"),
        processed = env("SMART_AGRI_HDFS_PROCESSED", s"$hdfsBase/processed"),
        streaming = env("SMART_AGRI_HDFS_STREAMING", s"$hdfsBase/streaming"),
        results = env("SMART_AGRI_HDFS_RESULTS", s"$hdfsBase/results")
      ),
      kafka = KafkaSettings(
        bootstrapServers = env("SMART_AGRI_KAFKA_BOOTSTRAP", "localhost:9092"),
        iotTopic = env("SMART_AGRI_KAFKA_IOT_TOPIC", "iot_sensor_readings")
      ),
      grid = GridSettings(
        cellsPerTileEdge = envInt("SMART_AGRI_GRID_CELLS_PER_EDGE", 100)
      )
    )
  }
}
