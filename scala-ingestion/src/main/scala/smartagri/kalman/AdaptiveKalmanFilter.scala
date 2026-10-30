package smartagri.kalman

/**
 * Pure, Spark-independent scalar Kalman filter: predict + update. No
 * Spark, no I/O -- directly unit-testable, including against a
 * hand-calculated example (see `AdaptiveKalmanFilterSpec`).
 *
 * STATE MODEL (documented per Task 5 -- NOT claimed as a novel Kalman
 * formulation; this is the textbook scalar random-walk model):
 *
 *     x_t = x_(t-1) + w_t,   w_t ~ N(0, Q_t)
 *     z_t = x_t + v_t,       v_t ~ N(0, R_t)
 *
 * `Q_t = config.processNoisePerDay * elapsedDays` -- process variance
 * accumulates over ELAPSED CALENDAR TIME, not per observation-step (see
 * `KalmanConfig`'s doc comment for why this matters for this project's
 * real, highly irregular Sentinel revisit gaps).
 *
 * The system-level novelty this project is investigating is the
 * ADAPTIVE RELIABILITY mechanism around `R_t` (see `ReliabilityEstimator`
 * and `stepAdaptive` below), not this filter's mathematics, which is
 * standard.
 */
object AdaptiveKalmanFilter {

  /**
   * Predict step: advances the state estimate by `elapsedDays` with no
   * new observation. `x` is unchanged (a pure random-walk process model
   * has no deterministic drift term); `p` grows by `Q_t`.
   */
  def predict(prior: KalmanState, elapsedDays: Int, config: KalmanConfig): KalmanState = {
    require(elapsedDays >= 0, s"elapsedDays must be >= 0, was $elapsedDays")
    val q = config.processNoisePerDay * elapsedDays
    KalmanState(x = prior.x, p = prior.p + q)
  }

  final case class UpdateResult(posterior: KalmanState, innovation: Double, kalmanGain: Double)

  /**
   * Update step: fuses one observation into a predicted state.
   *
   *     innovation = z - x_predicted
   *     K          = P_predicted / (P_predicted + R)
   *     x_posterior = x_predicted + K * innovation
   *     P_posterior = (1 - K) * P_predicted
   */
  def update(predicted: KalmanState, observation: Double, measurementVariance: Double): UpdateResult = {
    require(measurementVariance > 0.0, s"measurementVariance must be > 0, was $measurementVariance")
    val innovation = observation - predicted.x
    val gain = predicted.p / (predicted.p + measurementVariance)
    val posteriorX = predicted.x + gain * innovation
    val posteriorP = (1.0 - gain) * predicted.p
    UpdateResult(KalmanState(posteriorX, posteriorP), innovation, gain)
  }

  /** One observation and the measurement variance already assigned to it -- the unit `updateMultiSource`
    * fuses over (Task 5's multimodal-fusion architectural requirement). */
  final case class Observation(value: Double, measurementVariance: Double)

  /**
   * Fuses one OR MORE simultaneous observations via inverse-variance
   * weighting before a single update step -- the same fusion mathematics
   * this project's earlier (pre-Task-5) Kalman work used, generalized
   * here to any number of sources. With exactly one observation this
   * reduces exactly to `update(predicted, obs.value, obs.measurementVariance)`
   * (verified by a dedicated test) -- so a single-source Sentinel-only
   * pipeline and a future multi-source Sentinel+IoT pipeline share the
   * same code path rather than being two different implementations.
   */
  def updateMultiSource(predicted: KalmanState, observations: Seq[Observation]): UpdateResult = {
    require(observations.nonEmpty, "updateMultiSource requires at least one observation")
    val totalPrecision = observations.map(o => 1.0 / o.measurementVariance).sum
    val fusedValue = observations.map(o => o.value / o.measurementVariance).sum / totalPrecision
    val fusedVariance = 1.0 / totalPrecision
    update(predicted, fusedValue, fusedVariance)
  }

  /**
   * One full predict+update step in STATIC mode: measurement variance is
   * ALWAYS `config.baseMeasurementVariance`, regardless of any
   * reliability signal -- Task 5's "fixed measurement noise R, no
   * adaptive reliability" baseline mode.
   */
  def stepStatic(prior: KalmanState, elapsedDays: Int, observation: Double, config: KalmanConfig): (KalmanState, UpdateResult) = {
    val predicted = predict(prior, elapsedDays, config)
    val result = update(predicted, observation, config.baseMeasurementVariance)
    (predicted, result)
  }

  /**
   * One full predict+update step in ADAPTIVE mode: measurement variance
   * comes from `reliability` via `ReliabilityEstimator.reliabilityToMeasurementVariance`.
   *
   * At `reliability = 1.0` this produces EXACTLY the same measurement
   * variance as `stepStatic` (`baseMeasurementVariance / 1.0 ==
   * baseMeasurementVariance`), so the two modes agree exactly in that
   * case -- verified by a dedicated test, per Task 5's explicit
   * requirement.
   */
  def stepAdaptive(
      prior: KalmanState,
      elapsedDays: Int,
      observation: Double,
      reliability: Double,
      config: KalmanConfig
  ): (KalmanState, UpdateResult) = {
    val predicted = predict(prior, elapsedDays, config)
    val measurementVariance = ReliabilityEstimator.reliabilityToMeasurementVariance(reliability, config)
    val result = update(predicted, observation, measurementVariance)
    (predicted, result)
  }
}
