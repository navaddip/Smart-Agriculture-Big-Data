package smartagri.kalman

/**
 * The latent scalar vegetation-state estimate and its uncertainty, at
 * one point in time for one cell.
 *
 * `x` = latent vegetation condition/state (unitless, NDVI-like scale).
 * `p` = posterior variance of `x`.
 *
 * A SCALAR state, deliberately -- Task 5 explicitly prefers this for a
 * first implementation "if it makes the mathematics and validation
 * cleaner," which it does here: every filter equation stays a plain
 * `Double` operation, directly hand-checkable (see
 * `AdaptiveKalmanFilterSpec`'s hand-calculated example) rather than a
 * matrix operation that hides arithmetic mistakes.
 */
final case class KalmanState(x: Double, p: Double) {
  require(p >= 0.0, s"KalmanState variance must be non-negative, was $p")
}
