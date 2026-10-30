package smartagri.kalman

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.config.AppConfig

/**
 * Runs the per-cell chronological recursive Kalman filter over the
 * canonical historical dataset (Task 3B's output), in either STATIC or
 * ADAPTIVE mode.
 *
 * ==========================================================================
 * WHY THIS IS `mapPartitions`-BASED, NOT A SPARK `Window` FUNCTION
 * ==========================================================================
 * Task 4B's lag/rolling/baseline features are all expressible as
 * declarative `Window` aggregates because each one is a function of a
 * FIXED slice of INPUT rows (e.g. "the 4 rows before this one"). A
 * Kalman filter is different: each step's posterior depends on the
 * PREVIOUS STEP'S OWN OUTPUT (the posterior state and variance), which
 * is not a pre-existing input column Spark can express as a window
 * frame over -- it is produced by the recursion itself. There is no
 * `Window` aggregate that computes "apply this recursive update,
 * carrying forward my own prior output" -- so a genuinely sequential
 * computation is unavoidable.
 *
 * The chosen architecture keeps that sequential computation scalable
 * rather than collecting everything to the driver:
 *
 *   1. `repartition(tile, cell_row, cell_col)` -- Spark's hash
 *      partitioning guarantees every row for a given cell lands in the
 *      SAME partition (a partition typically holds many different
 *      cells). No cell's time series is ever split across partitions.
 *   2. `sortWithinPartitions(...)` -- establishes a deterministic row
 *      order within each partition as an optimization; `runCellSequential`
 *      below re-sorts by date defensively anyway (cheap, on one already-
 *      small cell's rows) rather than trusting the partition sort to
 *      survive the RDD conversion, so ordering correctness never
 *      silently depends on an internal assumption.
 *   3. `mapPartitions` -- executes ENTIRELY ON THE EXECUTOR, one
 *      partition at a time. `rowsIter.toList` materializes only that
 *      one partition's rows (bounded by ordinary Spark partition
 *      sizing), never the full dataset, and never on the driver. Rows
 *      are grouped by cell and each cell's own short sequence is
 *      filtered independently -- cell-level state never interacts
 *      across cells, and every cell's filter starts fresh from
 *      `config.initialStateMean/initialStateVariance`.
 *
 * This is the "partition-local sequential state propagation"
 * architecture Task 5's engineering-question section anticipated,
 * chosen because forcing a `Window`-based implementation here would not
 * merely be awkward -- it would be mathematically wrong, since the
 * recursion's own output can't be expressed as a window over
 * pre-existing columns.
 */
object KalmanFusionPipeline {

  final case class NdviObservation(tile: String, cellRow: Int, cellCol: Int, date: String, ndvi: Double)

  final case class KalmanOutputRow(
      tile: String,
      cellRow: Int,
      cellCol: Int,
      date: String,
      observationValue: Double,
      observationReliability: Double,
      measurementVariance: Double,
      kalmanPredictedState: Double,
      kalmanState: Double,
      kalmanVariance: Double,
      innovation: Double
  )

  /**
   * Rejects duplicate `(tile, cell_row, cell_col, date)` keys BEFORE any
   * filtering happens (Task 5's explicit requirement) -- a duplicate key
   * makes "chronological order" ambiguous for that cell, and silently
   * picking one would be exactly the kind of undocumented behavior this
   * project has consistently refused to do (see Task 3B's Sentinel-
   * deduplication precedent, which also stops rather than guesses on a
   * genuine conflict).
   */
  def validateNoDuplicateKeys(df: DataFrame): Unit = {
    val dupCount = df.groupBy("tile", "cell_row", "cell_col", "date").count().filter(col("count") > 1).count()
    if (dupCount > 0) {
      throw new IllegalStateException(
        s"Kalman input contains $dupCount duplicate (tile, cell_row, cell_col, date) key(s) -- " +
          "refusing to filter over an ambiguous chronological order. Deduplicate the input first."
      )
    }
  }

  /**
   * Runs the filter over the canonical dataset's NDVI observations only
   * (Task 5's "IoT availability" section: no real IoT stream exists yet
   * -- Task 6+ -- so this entry point processes the one genuinely
   * available direct-state observation source; the architecture is
   * still built to support more, see `AdaptiveKalmanFilter.updateMultiSource`
   * and its dedicated multi-source fixture test).
   */
  def runNdviOnly(spark: SparkSession, canonical: DataFrame, config: KalmanConfig, adaptive: Boolean): DataFrame = {
    validateNoDuplicateKeys(canonical)

    import spark.implicits._
    val obs = canonical
      .select(
        col("tile"),
        col("cell_row").cast("int").as("cellRow"),
        col("cell_col").cast("int").as("cellCol"),
        col("date"),
        col("ndvi").cast("double").as("ndvi")
      )
      .as[NdviObservation]

    val repartitioned = obs
      .repartition(col("tile"), col("cellRow"), col("cellCol"))
      .sortWithinPartitions(col("tile"), col("cellRow"), col("cellCol"), col("date"))

    // Rows arrive sorted by (cell, date), so each cell's series is a contiguous run. Only ONE
    // cell's series is buffered at a time: memory is bounded by the longest single-cell history,
    // never by partition size (no .toList of the partition, no collect() to the driver).
    val resultRDD = repartitioned.rdd.mapPartitions { (rowsIter: Iterator[NdviObservation]) =>
      val buffered = rowsIter.buffered
      Iterator.continually(()).takeWhile(_ => buffered.hasNext).flatMap { _ =>
        val first = buffered.next()
        val cell = scala.collection.mutable.ArrayBuffer(first)
        while (buffered.hasNext && buffered.head.tile == first.tile &&
               buffered.head.cellRow == first.cellRow && buffered.head.cellCol == first.cellCol)
          cell += buffered.next()
        runCellSequential(cell, config, adaptive)
      }
    }

    spark.createDataFrame(resultRDD)
  }

  /**
   * The sequential recursion itself, over ONE cell's observations.
   *
   * DEFENSIVELY re-sorts by `date` internally (`.sortBy(_.date)`) rather
   * than trusting that every caller has already sorted -- Task 5
   * requires chronological ordering to be actively ENFORCED, not merely
   * assumed from caller discipline, and this is exactly the kind of
   * silent-corruption risk (a future caller forgets to pre-sort, and
   * every downstream state becomes subtly wrong with no error raised)
   * this project has consistently guarded against elsewhere (e.g. Task
   * 2C's defense-in-depth fixes). `runNdviOnly`'s own
   * `sortWithinPartitions` call above is therefore a performance
   * optimization, not the correctness guarantee -- this line is.
   *
   * Pure Scala, no Spark API calls -- this is what makes it
   * independently unit-testable with tiny fixtures without needing a
   * `SparkSession` at all.
   */
  private[kalman] def runCellSequential(
      observations: Seq[NdviObservation],
      config: KalmanConfig,
      adaptive: Boolean
  ): Seq[KalmanOutputRow] = {
    val sortedObs = observations.sortBy(_.date)
    var state = KalmanState(config.initialStateMean, config.initialStateVariance)
    var priorDateOpt: Option[String] = None
    val priorNdviWindow = scala.collection.mutable.ArrayBuffer.empty[Double]

    sortedObs.map { o =>
      val elapsedDays = priorDateOpt match {
        case Some(priorDate) => daysBetween(priorDate, o.date)
        case None            => 0
      }

      val reliability =
        if (!adaptive) {
          1.0
        } else {
          val windowVals = priorNdviWindow.takeRight(config.reliabilityRollingWindowSize).toSeq
          val priorMean = if (windowVals.nonEmpty) Some(windowVals.sum / windowVals.length) else None
          val priorStd = if (windowVals.length >= 2) Some(stddevOf(windowVals)) else None
          ReliabilityEstimator.ndviReliability(o.ndvi, priorMean, priorStd, config)
        }

      val (predicted, updateResult) =
        if (adaptive) AdaptiveKalmanFilter.stepAdaptive(state, elapsedDays, o.ndvi, reliability, config)
        else AdaptiveKalmanFilter.stepStatic(state, elapsedDays, o.ndvi, config)

      val measurementVariance =
        if (adaptive) ReliabilityEstimator.reliabilityToMeasurementVariance(reliability, config)
        else config.baseMeasurementVariance

      state = updateResult.posterior
      priorDateOpt = Some(o.date)
      priorNdviWindow += o.ndvi

      KalmanOutputRow(
        tile = o.tile,
        cellRow = o.cellRow,
        cellCol = o.cellCol,
        date = o.date,
        observationValue = o.ndvi,
        observationReliability = reliability,
        measurementVariance = measurementVariance,
        kalmanPredictedState = predicted.x,
        kalmanState = updateResult.posterior.x,
        kalmanVariance = updateResult.posterior.p,
        innovation = updateResult.innovation
      )
    }
  }

  private[kalman] def daysBetween(d1: String, d2: String): Int =
    java.time.temporal.ChronoUnit.DAYS.between(java.time.LocalDate.parse(d1), java.time.LocalDate.parse(d2)).toInt

  private[kalman] def stddevOf(values: Seq[Double]): Double = {
    val mean = values.sum / values.length
    math.sqrt(values.map(v => (v - mean) * (v - mean)).sum / (values.length - 1))
  }

  def defaultCanonicalPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/canonical_historical.parquet"
}
