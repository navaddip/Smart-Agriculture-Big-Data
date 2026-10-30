package smartagri.integration

import smartagri.kalman.{AdaptiveKalmanFilter, KalmanConfig, KalmanState, ReliabilityEstimator}

/**
 * One observable IoT reading, already validated/parsed (Task 6/8),
 * ready to enter Kalman processing. Deliberately flat and minimal --
 * this type structurally CANNOT carry `trueValue`, `corruptionType`,
 * `corruptionSeverity`, or `sensorHealth`, because those fields simply
 * don't exist on it. That is the leakage guarantee for this adapter,
 * enforced by the type system, not by a runtime check.
 */
final case class KalmanObservationInput(
    tile: String,
    cellRow: Int,
    cellCol: Int,
    eventTime: String,
    value: Option[Double]
)

/**
 * One Kalman processing result for one observation.
 *
 * `observationReliability`, `measurementVariance`, and `innovation` are
 * `Option[Double]`, all `None` together, specifically for a MISSING
 * observation (`observedValue = None`): no update step happened (only a
 * predict step), so there is no reliability, no measurement variance,
 * and no innovation to report -- reporting a sentinel `0.0`/`NaN` there
 * would misrepresent "not applicable" as a real, if degenerate, value.
 */
final case class KalmanStreamOutput(
    tile: String,
    cellRow: Int,
    cellCol: Int,
    eventTime: String,
    sensorType: String,
    observedValue: Option[Double],
    kalmanPredictedState: Double,
    kalmanState: Double,
    kalmanVariance: Double,
    observationReliability: Option[Double],
    measurementVariance: Option[Double],
    innovation: Option[Double]
)

/**
 * Adapts observable IoT readings into the REAL Task 5
 * `AdaptiveKalmanFilter`/`ReliabilityEstimator` (no reimplementation,
 * no fake Kalman logic) -- per-cell, chronologically sequential, exactly
 * the recursion Task 5 already established, applied here to a different
 * physical quantity than Task 5's original NDVI-vigour use case.
 *
 * ==========================================================================
 * SENSOR SEMANTICS -- WHY ONLY `soil_moisture` IS WIRED INTO KALMAN HERE
 * ==========================================================================
 * Task 5's `KalmanState`/`AdaptiveKalmanFilter` are documented as
 * estimating a scalar "latent vegetation condition/state (unitless,
 * NDVI-like scale)". `soil_moisture`, `soil_temperature`, and
 * `air_temperature` are three physically DIFFERENT quantities from NDVI
 * vigour and from each other -- directly feeding all three into the
 * SAME vigour state, as if they were all measurements of one thing,
 * would be exactly the scientifically indefensible conflation Task 9
 * explicitly warns against.
 *
 * The `AdaptiveKalmanFilter`/`ReliabilityEstimator` objects themselves,
 * however, are generic scalar tools -- nothing in their function
 * signatures is NDVI-specific (a `Double` observation, a `Double`
 * reliability, a `KalmanConfig`). This adapter uses them to run a
 * SEPARATE, clearly-labeled Kalman track estimating a latent
 * SOIL-MOISTURE state, not the vigour state -- structurally identical
 * machinery, applied to `soil_moisture` observations on their own terms,
 * never merged with any vigour/NDVI processing. `soil_temperature` and
 * `air_temperature` are NOT wired into any Kalman track by this task --
 * they remain available as raw, contextual observable data (per Task 6's
 * own doc comment already anticipating this), not silently forced
 * through a filter that doesn't model their physical meaning. This is a
 * documented limitation/scope choice, not an oversight: extending Kalman
 * fusion to temperature would need its own, separately-configured
 * `KalmanConfig` (different bounds, different initial state) and is left
 * for a later task if wanted.
 *
 * `ReliabilityEstimator.ndviReliability` is reused AS-IS for
 * `soil_moisture` despite its name: the function itself only computes
 * "how far is this value from this cell's own recent local history" --
 * a generic deviation heuristic, not literally specific to NDVI's
 * physical meaning. Renaming it is out of scope for this integration
 * task (Task 9: do not modify Task 5 beyond a tiny compatibility fix).
 */
object StreamingKalmanAdapter {

  /** The one sensor type wired into Kalman processing by this task -- see the class doc comment. */
  val CompatibleSensorType: String = "soil_moisture"

  private def toCalendarDate(eventTimeIso: String): java.time.LocalDate =
    java.time.Instant.parse(eventTimeIso).atZone(java.time.ZoneOffset.UTC).toLocalDate

  /**
   * Elapsed whole calendar days between two event times, truncated to
   * UTC calendar dates -- Task 5's `AdaptiveKalmanFilter.predict` takes
   * `elapsedDays: Int` (a deliberately unmodified Task 5 signature, per
   * Task 9's "do not redesign the Kalman algorithm"), so sub-day IoT
   * event-time granularity is necessarily coarsened to whole days here.
   * DOCUMENTED LIMITATION: multiple same-day readings for one cell
   * therefore see `elapsedDays = 0` between them (no process-noise
   * growth within a day) -- a real granularity mismatch between Task 5's
   * daily-cadence design and IoT's potentially sub-daily cadence, stated
   * plainly rather than hidden. `.max(0)` guards against a negative
   * value if observations are ever processed out of order (callers are
   * still responsible for sorting first -- this is defense-in-depth,
   * not a substitute for that sort).
   */
  private def elapsedDays(priorEventTime: Option[String], eventTime: String): Int =
    priorEventTime match {
      case Some(prior) =>
        math.max(0, java.time.temporal.ChronoUnit.DAYS.between(toCalendarDate(prior), toCalendarDate(eventTime)).toInt)
      case None => 0
    }

  private def stddevOf(values: Seq[Double]): Double = {
    val mean = values.sum / values.length
    math.sqrt(values.map(v => (v - mean) * (v - mean)).sum / (values.length - 1))
  }

  /**
   * Processes ONE cell's observations, in the order given -- CALLERS
   * MUST PRE-SORT BY `eventTime` (unlike Task 5's own
   * `KalmanFusionPipeline.runCellSequential`, which defensively re-sorts
   * internally, this adapter does not, to avoid silently masking an
   * upstream ordering bug in a NEW integration path where that ordering
   * has not yet been established as reliable the way Task 5's own
   * Spark-based sort was -- `StreamingKalmanPipeline` sorts explicitly
   * before calling this, and that sort is itself tested).
   *
   * A MISSING observation (`value = None`) gets a PREDICT-ONLY step
   * (`AdaptiveKalmanFilter.predict`, no `update`) -- per Task 9's
   * explicit instruction never to manufacture a value or feed a missing
   * reading as zero. The state carries forward with only its variance
   * grown by elapsed time, exactly as it would for any calendar gap with
   * no observation at all.
   */
  def processCellSequential(
      sortedObservations: Seq[KalmanObservationInput],
      sensorType: String,
      config: KalmanConfig
  ): Seq[KalmanStreamOutput] = {
    var state = KalmanState(config.initialStateMean, config.initialStateVariance)
    var priorEventTime: Option[String] = None
    val priorValueWindow = scala.collection.mutable.ArrayBuffer.empty[Double]

    sortedObservations.map { obs =>
      val gapDays = elapsedDays(priorEventTime, obs.eventTime)
      priorEventTime = Some(obs.eventTime)

      obs.value match {
        case Some(v) =>
          val windowVals = priorValueWindow.takeRight(config.reliabilityRollingWindowSize).toSeq
          val priorMean = if (windowVals.nonEmpty) Some(windowVals.sum / windowVals.length) else None
          val priorStd = if (windowVals.length >= 2) Some(stddevOf(windowVals)) else None
          val reliability = ReliabilityEstimator.ndviReliability(v, priorMean, priorStd, config)
          val (predicted, result) = AdaptiveKalmanFilter.stepAdaptive(state, gapDays, v, reliability, config)
          val measurementVariance = ReliabilityEstimator.reliabilityToMeasurementVariance(reliability, config)

          state = result.posterior
          priorValueWindow += v

          KalmanStreamOutput(
            tile = obs.tile, cellRow = obs.cellRow, cellCol = obs.cellCol, eventTime = obs.eventTime,
            sensorType = sensorType, observedValue = Some(v),
            kalmanPredictedState = predicted.x, kalmanState = result.posterior.x, kalmanVariance = result.posterior.p,
            observationReliability = Some(reliability), measurementVariance = Some(measurementVariance),
            innovation = Some(result.innovation)
          )

        case None =>
          val predicted = AdaptiveKalmanFilter.predict(state, gapDays, config)
          state = predicted

          KalmanStreamOutput(
            tile = obs.tile, cellRow = obs.cellRow, cellCol = obs.cellCol, eventTime = obs.eventTime,
            sensorType = sensorType, observedValue = None,
            kalmanPredictedState = predicted.x, kalmanState = predicted.x, kalmanVariance = predicted.p,
            observationReliability = None, measurementVariance = None, innovation = None
          )
      }
    }
  }
}
