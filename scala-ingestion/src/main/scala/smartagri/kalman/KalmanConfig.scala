package smartagri.kalman

/**
 * Centralized, documented Kalman filter constants (Task 5: "avoid
 * arbitrary magic numbers... centralize in KalmanConfig"). Every
 * constant used anywhere in `smartagri.kalman` traces back to a named
 * field here, with its rationale documented below -- none of the
 * filter/reliability math has an unexplained inline literal.
 */
final case class KalmanConfig(
    initialStateMean: Double,
    initialStateVariance: Double,
    processNoisePerDay: Double,
    baseMeasurementVariance: Double,
    minimumReliability: Double,
    deviationEpsilon: Double,
    reliabilityRollingWindowSize: Int
)

object KalmanConfig {

  /**
   * Default configuration.
   *
   *   - `initialStateMean = 0.4`: a mid-range NDVI-scale starting guess
   *     for a cell with no observation history yet -- quickly revised by
   *     the first real observation regardless, since `initialStateVariance`
   *     is deliberately wide.
   *   - `initialStateVariance = 0.1`: wide on purpose -- there is
   *     genuinely no prior information before a cell's first observation,
   *     and a wide prior lets the first update trust the observation
   *     almost completely (a large `P` relative to `R` drives the Kalman
   *     gain close to 1).
   *   - `processNoisePerDay = 0.00016`: process variance accumulates PER
   *     ELAPSED CALENDAR DAY, not per observation-step -- this matters
   *     for this project's real data, where the Task 4A audit measured
   *     actual Sentinel revisit gaps ranging from 5 to 320 days for the
   *     same cell. Chosen so the most common real gap (5 days, Task 4A's
   *     measured median) accumulates to roughly the per-step process
   *     noise (0.0008) this project's earlier, pre-Task-5 Kalman work
   *     used for continuity -- `0.0008 / 5 = 0.00016`. A reasonable
   *     starting point kept for continuity, not an independently
   *     re-derived physical constant.
   *   - `baseMeasurementVariance = 0.0025`: the measurement variance at
   *     reliability = 1.0 (a fully-trusted observation) -- matches this
   *     project's previously-established satellite measurement variance
   *     (`R_SAT` in the pre-Task-5 `Analytics.scala`/`kalman_fusion.py`).
   *   - `minimumReliability = 0.05`: the reliability floor. Prevents
   *     `reliabilityToMeasurementVariance` from dividing by a value at or
   *     near zero and ensures no observation is ever fully discarded,
   *     only heavily down-weighted.
   *   - `deviationEpsilon = 0.01`: added to a rolling standard deviation
   *     before it becomes a denominator in the reliability deviation
   *     signal, so a perfectly flat recent history (std = 0) doesn't
   *     produce a division by zero.
   *   - `reliabilityRollingWindowSize = 4`: matches Task 4B's own
   *     rolling-window size choice, kept consistent across the project.
   */
  val default: KalmanConfig = KalmanConfig(
    initialStateMean = 0.4,
    initialStateVariance = 0.1,
    processNoisePerDay = 0.00016,
    baseMeasurementVariance = 0.0025,
    minimumReliability = 0.05,
    deviationEpsilon = 0.01,
    reliabilityRollingWindowSize = 4
  )
}
