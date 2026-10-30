package smartagri.kalman

import smartagri.model.SentinelObservation

/**
 * Deterministic reliability estimation from observable evidence only.
 *
 * CRITICAL, per Task 5: this object never references, and must never be
 * modified to reference, `sensor_health`, corruption labels/severity/
 * metadata, any future observation, or any Kalman/ML/conformal/decision
 * output. Every signal used here is computable from information that
 * genuinely exists at observation time -- the observation's own value,
 * and a strictly-prior rolling window of the same cell's earlier
 * observations, which the caller (`KalmanFusionPipeline`) is responsible
 * for maintaining with the same strictly-prior discipline established in
 * Task 4B.
 */
object ReliabilityEstimator {

  /** Reused, not retyped -- the same canonical NDVI validity bounds established in Task 1. */
  private val NdviMin = SentinelObservation.NdviMin
  private val NdviMax = SentinelObservation.NdviMax

  /**
   * Computes an observable-evidence reliability score in
   * `[config.minimumReliability, 1.0]` for one NDVI observation.
   *
   * Two observable signals, per Task 5's suggested list for Sentinel/NDVI:
   *
   *   1. RANGE VALIDITY: a value outside `[-1, 1]` is physically
   *      impossible and immediately gets the reliability floor,
   *      regardless of the deviation signal below.
   *   2. LOCAL DEVIATION ("abrupt change relative to recent prior
   *      observations"): `|value - priorMean| / (priorStd + epsilon)`,
   *      a z-score-like measure of how surprising this observation is
   *      relative to the cell's own recent trajectory. Reliability
   *      falls off smoothly and monotonically as deviation grows, via
   *      `reliability = 1 / (1 + deviation)`, then clipped to the
   *      documented floor/ceiling.
   *
   * This deviation signal is a HYPOTHESIS about what makes an
   * observation trustworthy (Task 5's explicit framing: not claimed to
   * improve anything until a controlled corruption experiment
   * demonstrates it) -- a genuinely unusual but real rapid change in
   * vegetation state would also score as "less reliable" here, which is
   * a real, acknowledged limitation of this heuristic, not an oversight.
   *
   * `priorMean`/`priorStd` are `None` when a cell has no prior
   * observations yet (or only one, for `priorStd`) -- in that case this
   * function returns a neutral, fully-trusted `1.0` rather than
   * guessing at a deviation signal it cannot actually compute.
   */
  def ndviReliability(
      value: Double,
      priorMean: Option[Double],
      priorStd: Option[Double],
      config: KalmanConfig
  ): Double = {
    if (value < NdviMin || value > NdviMax) {
      config.minimumReliability
    } else {
      (priorMean, priorStd) match {
        case (Some(mean), Some(std)) =>
          val deviation = math.abs(value - mean) / (std + config.deviationEpsilon)
          val raw = 1.0 / (1.0 + deviation)
          math.max(config.minimumReliability, math.min(1.0, raw))
        case _ =>
          1.0
      }
    }
  }

  /**
   * Maps a reliability score to a measurement variance (Task 5's
   * "reliability -> Kalman mapping"): `R_t = R_base / max(reliability,
   * minimumReliability)`.
   *
   * Monotonic by construction: higher reliability -> smaller
   * denominator's reciprocal effect is smaller -> lower `R_t` -> more
   * trust (a smaller measurement variance means the Kalman gain favors
   * the observation more). Lower reliability -> larger `R_t` -> less
   * influence on the posterior. The `max(reliability, minimumReliability)`
   * guard is defensive: `ndviReliability` already floors its output at
   * `minimumReliability`, so this only matters for a reliability value
   * supplied from elsewhere (e.g. a future non-NDVI source) that hasn't
   * already been floored.
   */
  /**
   * General form of the observable reliability signal, for any sensor with physical bounds
   * [lower, upper]. The two components can be switched off independently -- used by the
   * ablation study (smartagri.kalman.RobustnessExperiment) to measure what each contributes:
   *   - useBounds:    a physically impossible value gets minimum reliability
   *   - useDeviation: reliability = 1 / (1 + |x - recentMean| / (recentStd + eps))
   * Neither component ever looks at ground truth.
   */
  def reliability(
      value: Double,
      lower: Double,
      upper: Double,
      priorMean: Option[Double],
      priorStd: Option[Double],
      config: KalmanConfig,
      useBounds: Boolean = true,
      useDeviation: Boolean = true
  ): Double = {
    if (useBounds && (value < lower || value > upper)) config.minimumReliability
    else if (!useDeviation) 1.0
    else (priorMean, priorStd) match {
      case (Some(mean), Some(std)) =>
        val deviation = math.abs(value - mean) / (std + config.deviationEpsilon)
        math.max(config.minimumReliability, math.min(1.0, 1.0 / (1.0 + deviation)))
      case _ => 1.0
    }
  }

  def reliabilityToMeasurementVariance(reliability: Double, config: KalmanConfig): Double =
    config.baseMeasurementVariance / math.max(reliability, config.minimumReliability)
}
