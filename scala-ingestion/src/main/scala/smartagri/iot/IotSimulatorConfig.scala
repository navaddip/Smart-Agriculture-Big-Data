package smartagri.iot

/**
 * Configuration for one simulated sensor variable (Task 6: "central
 * configuration... do not scatter constants throughout the simulator").
 *
 * The temporal model is an AR(1)-like process:
 *
 *     x_t = mu + persistence * (x_(t-1) - mu) + eps_t,   eps_t ~ N(0, processNoiseStd^2)
 *
 * where `mu = baselineMean + <per-cell spatial offset>` (see
 * `SpatialVariation`). `persistence` (`phi`) must be in `(0, 1)`:
 * closer to 1 means the series drifts slowly and smoothly; closer to 0
 * means it reverts to `mu` almost immediately after each step. This is
 * a standard, simple autocorrelated process -- explicitly a SYNTHETIC
 * simulator's temporal model, not a calibrated physical crop/soil
 * model, and not claimed to be one.
 *
 * The observation model is separate from the process model, per Task 6:
 *
 *     observed_t = x_t + measurementNoise_t,   measurementNoise_t ~ N(0, measurementNoiseStd^2)
 *
 * `processNoiseStd` governs how much the TRUE environmental state
 * itself varies step to step; `measurementNoiseStd` governs how much
 * additional error the SENSOR introduces on top of the true state when
 * reporting it. Keeping these separate matters specifically because
 * Task 5's Kalman filter models process and measurement uncertainty as
 * two distinct things, and a simulator that conflated them would make
 * that distinction untestable against a known ground truth.
 */
final case class SensorTypeConfig(
    name: String,
    baselineMean: Double,
    spatialVariationRange: Double,
    persistence: Double,
    processNoiseStd: Double,
    measurementNoiseStd: Double,
    lowerBound: Double,
    upperBound: Double
) {
  require(persistence > 0.0 && persistence < 1.0, s"persistence must be in (0,1), was $persistence")
  require(lowerBound < upperBound, s"lowerBound ($lowerBound) must be < upperBound ($upperBound)")
}

/**
 * Controlled-corruption configuration (Task 6: "NORMAL MODE VS
 * CORRUPTION MODE"). All probabilities are per-observation and
 * independent of each other; `IotSimulatorConfig.NoCorruption` (all
 * zeros) is the default, so ordinary generation is corruption-free
 * unless explicitly configured otherwise.
 *
 * Five modes, matching Task 6's list exactly:
 *   - missing: the observable `value` becomes `None` entirely.
 *   - additive_noise: extra Gaussian noise beyond ordinary measurement
 *     noise is added to the observed value.
 *   - outlier: the observed value is pushed `outlierMagnitude` away
 *     from what it would otherwise be, in a random direction -- and,
 *     UNLIKE ordinary generation, is deliberately allowed to exceed the
 *     sensor's normal physical bounds, since a malfunctioning real
 *     sensor genuinely can report a physically implausible value; this
 *     is an intentional, documented exception to the "every generated
 *     variable must have defensible bounds" rule for NORMAL generation.
 *   - bias: a constant offset is added (still clipped to physical
 *     bounds, since a biased-but-still-reporting sensor is a different,
 *     more subtle failure mode than an outlier glitch).
 *   - stale: the observed value is replaced with the sensor's own
 *     previous observed value (simulating a sensor stuck reporting its
 *     last reading), only when a previous observed value actually
 *     exists.
 */
final case class CorruptionConfig(
    missingProbability: Double,
    additiveNoiseProbability: Double,
    additiveNoiseStd: Double,
    outlierProbability: Double,
    outlierMagnitude: Double,
    biasProbability: Double,
    biasMagnitude: Double,
    staleProbability: Double
) {
  Seq(missingProbability, additiveNoiseProbability, outlierProbability, biasProbability, staleProbability).foreach { p =>
    require(p >= 0.0 && p <= 1.0, s"corruption probabilities must be in [0,1], found $p")
  }
}

/**
 * Top-level simulator configuration.
 *
 * `intervalSeconds` is the nominal gap between an event and the next
 * (REGULAR sampling, Task 6's option A). `irregularIntervalJitterSeconds
 * = 0` means regular sampling exactly; a positive value adds bounded
 * jitter to each step (option B), always kept strictly less than
 * `intervalSeconds` so the sequence of event timestamps remains
 * strictly increasing (never zero or negative gaps, which would create
 * an ambiguous or duplicate event ordering).
 */
final case class IotSimulatorConfig(
    seed: Long,
    observationsPerCell: Int,
    startEventTime: String,
    intervalSeconds: Long,
    irregularIntervalJitterSeconds: Long,
    sensorTypes: Seq[SensorTypeConfig],
    corruption: CorruptionConfig
) {
  require(observationsPerCell > 0, s"observationsPerCell must be > 0, was $observationsPerCell")
  require(intervalSeconds > 0, s"intervalSeconds must be > 0, was $intervalSeconds")
  require(
    irregularIntervalJitterSeconds >= 0 && irregularIntervalJitterSeconds < intervalSeconds,
    s"irregularIntervalJitterSeconds ($irregularIntervalJitterSeconds) must be in [0, intervalSeconds) " +
      s"so consecutive event timestamps never collide or go backward"
  )
  require(sensorTypes.nonEmpty, "at least one sensor type must be configured")
}

object IotSimulatorConfig {

  /** All corruption probabilities at zero -- ordinary, uncorrupted generation. */
  val NoCorruption: CorruptionConfig =
    CorruptionConfig(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

  /**
   * The three recommended sensor types (Task 6's "recommended minimum"),
   * with illustrative parameter values -- starting points for a
   * synthetic simulator, not measured/calibrated physical constants:
   *
   *   - soil_moisture: volumetric fraction, bounded [0,1]; slow-moving
   *     (persistence=0.85) since soil moisture changes gradually between
   *     rain/irrigation events.
   *   - soil_temperature: degrees C, bounded [-10,50]; even slower
   *     (persistence=0.9) and lower-noise than air temperature, since
   *     soil buffers temperature swings.
   *   - air_temperature: degrees C, bounded [-10,50]; more volatile
   *     (persistence=0.7, larger processNoiseStd) since air temperature
   *     fluctuates faster than soil.
   */
  val defaultSensorTypes: Seq[SensorTypeConfig] = Seq(
    SensorTypeConfig(
      name = "soil_moisture", baselineMean = 0.25, spatialVariationRange = 0.05,
      persistence = 0.85, processNoiseStd = 0.01, measurementNoiseStd = 0.01,
      lowerBound = 0.0, upperBound = 1.0
    ),
    SensorTypeConfig(
      name = "soil_temperature", baselineMean = 22.0, spatialVariationRange = 2.0,
      persistence = 0.9, processNoiseStd = 0.5, measurementNoiseStd = 0.3,
      lowerBound = -10.0, upperBound = 50.0
    ),
    SensorTypeConfig(
      name = "air_temperature", baselineMean = 26.0, spatialVariationRange = 3.0,
      persistence = 0.7, processNoiseStd = 1.0, measurementNoiseStd = 0.5,
      lowerBound = -10.0, upperBound = 50.0
    )
  )

  /** A ready-to-use default configuration for a given seed -- regular hourly sampling, no corruption. */
  def default(seed: Long): IotSimulatorConfig = IotSimulatorConfig(
    seed = seed,
    observationsPerCell = 20,
    startEventTime = "2024-01-01T00:00:00Z",
    intervalSeconds = 3600,
    irregularIntervalJitterSeconds = 0,
    sensorTypes = defaultSensorTypes,
    corruption = NoCorruption
  )
}
