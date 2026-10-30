package smartagri.iot

import scala.util.Random

/**
 * Pure, Spark-independent generation of one deterministic time series
 * for one (cell, sensorType) -- no I/O, no Spark, directly unit-testable.
 */
object SensorSeriesGenerator {

  /** One generated point: the TRUE environmental state, and what a real sensor would actually
    * report (`observedValue`, `None` if this reading is missing) -- kept together here only because
    * this is the generation step itself; callers split these into the separate observable and
    * ground-truth outputs immediately afterward (see `IotSimulatorPipeline`), never mixing them into
    * one persisted record. */
  final case class GeneratedPoint(
      trueValue: Double,
      observedValue: Option[Double],
      corruptionType: Option[String],
      corruptionSeverity: Option[Double]
  )

  /**
   * Generates `count` points for one (cell, sensorType), given a
   * deterministic `derivedSeed` (see `DeterministicSeed`).
   *
   * Process step (AR(1), see `SensorTypeConfig`'s doc comment for the
   * full model): `x_t = mu + persistence*(x_(t-1)-mu) + eps_t`, clipped
   * to `[lowerBound, upperBound]` after each step -- the clip is the
   * simulator's chosen strategy for keeping the TRUE state physically
   * plausible (Task 6: "generate -> validate -> handle invalid result
   * deterministically"; clipping is that deterministic handling,
   * documented here rather than left implicit).
   *
   * Observation step: `observed_t = clip(x_t + measurementNoise_t)`,
   * with `measurementNoise_t` drawn independently of the process noise
   * `eps_t` (same `rng`, but a separate `nextGaussian()` draw) -- process
   * and measurement noise are two distinct random draws with two
   * distinct configured magnitudes, never conflated into one.
   *
   * Corruption (if `corruption` has any non-zero probability) is then
   * applied on top of the observation step's result, per
   * `CorruptionConfig`'s documented mode list and fixed check order
   * (missing, outlier, bias, additive_noise, stale) -- a fixed order
   * matters for reproducibility: the same seed must always roll the
   * same sequence of `rng.nextDouble()` calls in the same order.
   */
  def generateSeries(
      derivedSeed: Long,
      baselineMean: Double,
      spatialOffset: Double,
      sensorConfig: SensorTypeConfig,
      corruption: CorruptionConfig,
      count: Int
  ): Seq[GeneratedPoint] = {
    require(count > 0, s"count must be > 0, was $count")
    val rng = new Random(derivedSeed)
    val mu = baselineMean + spatialOffset

    val builder = Seq.newBuilder[GeneratedPoint]
    var priorTrue = mu
    var lastObserved: Option[Double] = None

    var i = 0
    while (i < count) {
      val eps = rng.nextGaussian() * sensorConfig.processNoiseStd
      val trueRaw = mu + sensorConfig.persistence * (priorTrue - mu) + eps
      val trueValue = clip(trueRaw, sensorConfig.lowerBound, sensorConfig.upperBound)
      priorTrue = trueValue

      val measurementNoise = rng.nextGaussian() * sensorConfig.measurementNoiseStd
      val rawObserved = clip(trueValue + measurementNoise, sensorConfig.lowerBound, sensorConfig.upperBound)

      val point = applyCorruption(rng, trueValue, rawObserved, lastObserved, corruption, sensorConfig)
      lastObserved = point.observedValue.orElse(lastObserved)
      builder += point
      i += 1
    }
    builder.result()
  }

  private def clip(v: Double, lo: Double, hi: Double): Double = math.max(lo, math.min(hi, v))

  private def applyCorruption(
      rng: Random,
      trueValue: Double,
      rawObserved: Double,
      lastObserved: Option[Double],
      corruption: CorruptionConfig,
      sensorConfig: SensorTypeConfig
  ): GeneratedPoint = {
    // Fixed, documented check order -- every branch consumes exactly one rng.nextDouble() (plus, for
    // outlier, one extra nextBoolean() for direction), so the total number and order of random draws
    // per point is always the same regardless of which mode (if any) actually fires -- required for
    // reproducibility to hold even when corruption is configured differently between two runs that
    // otherwise share a seed prefix.
    if (rng.nextDouble() < corruption.missingProbability) {
      GeneratedPoint(trueValue, None, Some("missing"), None)
    } else if (rng.nextDouble() < corruption.outlierProbability) {
      val direction = if (rng.nextBoolean()) 1.0 else -1.0
      // Deliberately NOT clipped to [lowerBound, upperBound] -- see CorruptionConfig's doc comment.
      val outlierValue = rawObserved + direction * corruption.outlierMagnitude
      GeneratedPoint(trueValue, Some(outlierValue), Some("outlier"), Some(corruption.outlierMagnitude))
    } else if (rng.nextDouble() < corruption.biasProbability) {
      val biasedValue = clip(rawObserved + corruption.biasMagnitude, sensorConfig.lowerBound, sensorConfig.upperBound)
      GeneratedPoint(trueValue, Some(biasedValue), Some("bias"), Some(corruption.biasMagnitude))
    } else if (rng.nextDouble() < corruption.additiveNoiseProbability) {
      val extraNoise = rng.nextGaussian() * corruption.additiveNoiseStd
      val noisyValue = clip(rawObserved + extraNoise, sensorConfig.lowerBound, sensorConfig.upperBound)
      GeneratedPoint(trueValue, Some(noisyValue), Some("additive_noise"), Some(corruption.additiveNoiseStd))
    } else if (rng.nextDouble() < corruption.staleProbability && lastObserved.isDefined) {
      GeneratedPoint(trueValue, lastObserved, Some("stale"), None)
    } else {
      GeneratedPoint(trueValue, Some(rawObserved), None, None)
    }
  }
}
