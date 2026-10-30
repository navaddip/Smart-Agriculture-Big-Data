package smartagri.iot

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.config.AppConfig

/**
 * Scalable, partition-oriented generation of simulated IoT readings
 * across many canonical cells.
 *
 * SCALABILITY (Task 6's explicit requirement): the number of
 * cells x observationsPerCell x sensorTypes must be able to grow
 * without collecting the full generated dataset to the driver. This
 * mirrors Task 5's `KalmanFusionPipeline` architecture directly: cells
 * are distributed across Spark partitions, and each partition generates
 * its own cells' readings independently via `mapPartitions`, using a
 * lazy `Iterator` the whole way through (`.iterator.flatMap`, never
 * `.toList`/`.toSeq` materializing an entire partition's worth of
 * generated readings before Spark can start consuming them). No
 * `.collect()` is called anywhere in this object. No Python UDF is
 * used -- generation is plain Scala inside `mapPartitions`.
 */
object IotSimulatorPipeline {

  /** One canonical cell to generate readings for. */
  final case class CellKey(tile: String, cellRow: Int, cellCol: Int)

  /**
   * Generates BOTH the observable and ground-truth datasets for the
   * given set of cells in ONE pass (avoiding generating the same series
   * twice) using `mapPartitions`, and caches the combined intermediate
   * result so deriving the two final DataFrames from it doesn't force
   * two separate full recomputations.
   */
  def generate(spark: SparkSession, cells: Dataset[CellKey], config: IotSimulatorConfig): (DataFrame, DataFrame) = {
    import spark.implicits._

    val repartitioned = cells.repartition(col("tile"), col("cellRow"), col("cellCol"))

    val combined = repartitioned.mapPartitions { cellIter =>
      cellIter.flatMap { cellKey =>
        config.sensorTypes.iterator.flatMap { sensorConfig =>
          val valueSeed = DeterministicSeed.forCellAndSensor(config.seed, cellKey.tile, cellKey.cellRow, cellKey.cellCol, sensorConfig.name)
          val timingSeed = DeterministicSeed.forEventTiming(valueSeed)
          val spatialOffset = SpatialVariation.offsetFor(valueSeed, sensorConfig.spatialVariationRange)

          val series = SensorSeriesGenerator.generateSeries(
            valueSeed, sensorConfig.baselineMean, spatialOffset, sensorConfig, config.corruption, config.observationsPerCell
          )
          val eventTimes = EventTimeGenerator.generate(
            config.startEventTime, config.intervalSeconds, config.irregularIntervalJitterSeconds, config.observationsPerCell, timingSeed
          )
          val sid = IotSchema.sensorId(cellKey.tile, cellKey.cellRow, cellKey.cellCol, sensorConfig.name)

          series.iterator.zip(eventTimes.iterator).map { case (point, eventTime) =>
            (
              ObservableReading(sid, cellKey.tile, cellKey.cellRow, cellKey.cellCol, eventTime, sensorConfig.name, point.observedValue),
              GroundTruthReading(
                sid, cellKey.tile, cellKey.cellRow, cellKey.cellCol, eventTime, sensorConfig.name,
                point.trueValue, point.observedValue, point.corruptionType, point.corruptionSeverity
              )
            )
          }
        }
      }
    }
    combined.cache()

    val observable = combined.map(_._1).toDF()
    val groundTruth = combined.map(_._2).toDF()
    (observable, groundTruth)
  }

  /**
   * Rejects a duplicate `(sensor_id, event_time)` key -- deterministic
   * event-identity handling (Task 6 test item 18): under normal
   * operation this can never fire (event times are always strictly
   * increasing per sensor, enforced by `IotSimulatorConfig`'s jitter
   * bound), but is checked explicitly rather than assumed, matching
   * this project's established "verify, don't assume" pattern (e.g.
   * Task 5's `validateNoDuplicateKeys`).
   */
  def validateNoDuplicateEventKeys(observable: DataFrame): Unit = {
    val dupCount = observable.groupBy("sensorId", "eventTime").count().filter(col("count") > 1).count()
    if (dupCount > 0) {
      throw new IllegalStateException(
        s"IoT observable output contains $dupCount duplicate (sensorId, eventTime) key(s) -- " +
          "event identity must be unique and deterministic."
      )
    }
  }

  def defaultObservableOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/iot"

  def defaultGroundTruthOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/iot_eval"

  /** Derives `event_date` (the calendar date portion of `eventTime`) as the partitioning column for
    * output -- Task 6's suggested partitioning, consistent with the rest of the project's date
    * partitioning convention (Task 2A/3B/4B all partition by date). */
  def withEventDate(df: DataFrame): DataFrame =
    df.withColumn("event_date", substring(col("eventTime"), 1, 10))
}
