package smartagri.canonical

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.config.AppConfig

/**
 * Builds the canonical historical dataset (Task 3B, sections E-J):
 * deduplicated Sentinel-2 NDVI, joined to ERA5 (via nearest-neighbour
 * spatial alignment + exact-date match) and to repaired SoilGrids (via
 * the direct 1:1 spatial join already verified in the Task 3A audit).
 *
 * Grain: one row per `(tile, cell_row, cell_col, date)` -- one Sentinel
 * observation. Driven entirely by which Sentinel observations exist;
 * SoilGrids and ERA5 are always attached TO a Sentinel row, never the
 * reverse, so this job can never invent rows that don't correspond to a
 * real satellite observation.
 */
object CanonicalHistoricalDatasetBuilder {

  /** Exact column order Task 3B section E specifies. */
  val RequiredColumns: Seq[String] = Seq(
    // identity
    "tile", "cell_row", "cell_col", "date", "lat", "lon",
    // sentinel
    "ndvi",
    // era5
    "swvl1", "swvl2", "swvl3", "swvl4", "u10", "v10", "t2m", "d2m", "ssrd", "e", "tp", "pev",
    // soilgrids
    "ph_water", "nitrogen_g_kg", "soil_organic_carbon", "clay_pct", "sand_pct", "silt_pct", "water_content_fc",
    // provenance / quality -- deliberately nothing beyond what Task 3B section E allows:
    // no sensor_health, no reliability scores, no Kalman state, no ML/conformal outputs.
    "sentinel_dedup_applied", "soilgrids_nodata_repaired", "era5_match_distance_km",
    "era5_source_lat", "era5_source_lon"
  )

  /**
   * Extra quality / derived columns produced by the Scala preprocessing jobs
   * (smartagri.preprocess.*). Carried through when present so the Kalman
   * reliability model and the reports can use them; never required, so the
   * builder still accepts older inputs that lack them.
   */
  val OptionalColumns: Seq[String] = Seq(
    // Sentinel-2 pixel QA
    "ndvi_std", "clear_fraction", "cloud_fraction", "water_fraction",
    // ERA5 derived agro-meteorology
    "t2m_c", "t2m_max_c", "t2m_min_c", "rh_pct", "vpd_kpa", "wind_speed_ms", "tp_mm",
    // SoilGrids provenance
    "soil_fill_level", "soil_in_extent", "texture_class"
  )

  final case class BuildResult(
      sentinelDedup: SentinelDeduplication.DedupResult,
      soilgridsRepair: SoilGridsNodataRepair.RepairResult,
      era5Alignment: Era5SpatialAlignment.AlignmentResult,
      canonicalRowCount: Long,
      distinctKeyCount: Long,
      tileCount: Long,
      dateCount: Long
  )

  def build(
      spark: SparkSession,
      sentinelRaw: DataFrame,
      era5Raw: DataFrame,
      soilgridsRaw: DataFrame
  ): (DataFrame, BuildResult) = {

    val (sentinelDeduped, dedupResult) = SentinelDeduplication.deduplicate(sentinelRaw)
    val (soilgridsRepaired, repairResult) = SoilGridsNodataRepair.repair(soilgridsRaw)
    val (withEra5, alignmentResult) = Era5SpatialAlignment.align(spark, sentinelDeduped, era5Raw)

    // SoilGrids joins directly on (tile, cell_row, cell_col) -- verified 1:1 with Sentinel's grid
    // in the Task 3A audit. lat/lon are dropped from the SoilGrids side first: Sentinel's lat/lon
    // (the grid-defining source, per Task 1) remain the sole, authoritative identity columns --
    // SoilGrids' own lat/lon (sampled at the same centroids) would otherwise collide by name.
    val soilgridsForJoin = soilgridsRepaired.drop("lat", "lon")
    val withSoil = withEra5.join(soilgridsForJoin, Seq("tile", "cell_row", "cell_col"), "left")

    val extras = OptionalColumns.filter(withSoil.columns.contains)
    val canonical = withSoil
      .withColumn("sentinel_dedup_applied", lit(true))
      .withColumn("soilgrids_nodata_repaired", lit(true))
      .withColumn("weather_available", col("swvl1").isNotNull)
      .select((RequiredColumns ++ extras :+ "weather_available").map(col): _*)

    canonical.cache()

    val canonicalRowCount = canonical.count()
    val distinctKeyCount = canonical.dropDuplicates(Seq("tile", "cell_row", "cell_col", "date")).count()
    val tileCount = canonical.select("tile").distinct().count()
    val dateCount = canonical.select("date").distinct().count()

    val result = BuildResult(
      sentinelDedup = dedupResult,
      soilgridsRepair = repairResult,
      era5Alignment = alignmentResult,
      canonicalRowCount = canonicalRowCount,
      distinctKeyCount = distinctKeyCount,
      tileCount = tileCount,
      dateCount = dateCount
    )

    (canonical, result)
  }

  /**
   * Section F/G/H validation checks. Returns a list of failure messages
   * (empty = all checks passed). Never silently ignores a failed check --
   * per Task 3B: "If this equality does not hold: STOP and diagnose
   * before continuing."
   */
  def validate(canonical: DataFrame, result: BuildResult, soilgridsRepaired: DataFrame): Seq[String] = {
    val failures = scala.collection.mutable.ListBuffer.empty[String]

    // F: canonical_rows == deduplicated_sentinel_rows
    if (result.canonicalRowCount != result.sentinelDedup.finalRows) {
      failures += s"canonical row count (${result.canonicalRowCount}) != deduplicated Sentinel row count (${result.sentinelDedup.finalRows})"
    }
    // F: zero duplicate canonical keys
    if (result.distinctKeyCount != result.canonicalRowCount) {
      failures += s"canonical table has duplicate (tile,cell_row,cell_col,date) keys: distinct=${result.distinctKeyCount}, total=${result.canonicalRowCount}"
    }
    // F: NDVI within [-1, 1]
    val ndviOutOfRange = canonical.filter(col("ndvi").isNotNull && (col("ndvi") < -1.0 || col("ndvi") > 1.0)).count()
    if (ndviOutOfRange > 0) {
      failures += s"$ndviOutOfRange canonical rows have NDVI outside [-1, 1]"
    }
    // G: no duplicate SoilGrids keys
    val soilKeyDupes = soilgridsRepaired
      .groupBy("tile", "cell_row", "cell_col")
      .count()
      .filter(col("count") > 1)
      .count()
    if (soilKeyDupes > 0) {
      failures += s"SoilGrids has $soilKeyDupes duplicate (tile,cell_row,cell_col) keys"
    }
    // H: exactly one ERA5 attachment per row (checked via era5Alignment's matched/unmatched split
    // already recorded in result; re-verified here directly against the built table)
    val missingEra5 = canonical.filter(col("swvl1").isNull).count()
    if (missingEra5 != result.era5Alignment.unmatchedRows) {
      failures += s"ERA5-missing row count in canonical table ($missingEra5) does not match alignment's own unmatchedRows (${result.era5Alignment.unmatchedRows})"
    }

    failures.toSeq
  }

  def defaultOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/canonical_historical.parquet"

  def defaultSentinelPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/sentinel2_ndvi.parquet"

  def defaultEra5Path(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/era5_weather.parquet"

  def defaultSoilgridsPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/soilgrids.parquet"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CanonicalHistoricalDatasetBuilder").getOrCreate()
    try {
      val config = AppConfig.default
      val sentinelPath = if (args.length >= 1) args(0) else defaultSentinelPath(config)
      val era5Path = if (args.length >= 2) args(1) else defaultEra5Path(config)
      val soilgridsPath = if (args.length >= 3) args(2) else defaultSoilgridsPath(config)
      val outputPath = if (args.length >= 4) args(3) else defaultOutputPath(config)

      val sentinelRaw = spark.read.parquet(sentinelPath)
      val era5Raw = spark.read.parquet(era5Path)
      val soilgridsRaw = spark.read.parquet(soilgridsPath)

      val (soilgridsRepaired, _) = SoilGridsNodataRepair.repair(soilgridsRaw)
      val (canonical, result) = build(spark, sentinelRaw, era5Raw, soilgridsRaw)

      val failures = validate(canonical, result, soilgridsRepaired)
      if (failures.nonEmpty) {
        System.err.println("CANONICAL DATASET VALIDATION FAILED:")
        failures.foreach(f => System.err.println(s"  - $f"))
        System.exit(1)
      }

      canonical.write.mode("overwrite").partitionBy("date").parquet(outputPath)

      println(s"Canonical historical dataset written to $outputPath")
      println(s"Result: $result")
    } finally {
      spark.stop()
    }
  }
}
