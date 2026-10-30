package smartagri.kalman

import org.scalatest.funsuite.AnyFunSuite

class AdaptiveKalmanFilterSpec extends AnyFunSuite {

  private val config = KalmanConfig.default

  // -- 1. Prediction step correctness ---------------------------------------

  test("predict advances P by processNoisePerDay * elapsedDays and leaves x unchanged") {
    val prior = KalmanState(x = 0.5, p = 0.04)
    val predicted = AdaptiveKalmanFilter.predict(prior, elapsedDays = 10, config)
    assert(predicted.x == 0.5)
    assert(math.abs(predicted.p - (0.04 + config.processNoisePerDay * 10)) < 1e-12)
  }

  test("predict with elapsedDays=0 leaves P unchanged (no time has passed)") {
    val prior = KalmanState(x = 0.5, p = 0.04)
    val predicted = AdaptiveKalmanFilter.predict(prior, elapsedDays = 0, config)
    assert(predicted.p == 0.04)
  }

  test("predict rejects a negative elapsedDays") {
    assertThrows[IllegalArgumentException] {
      AdaptiveKalmanFilter.predict(KalmanState(0.5, 0.04), elapsedDays = -1, config)
    }
  }

  // -- 2. Update step correctness against a hand-calculated scalar example -------

  test("update matches a hand-calculated example exactly") {
    // Hand calculation:
    //   x_pred = 0.5, P_pred = 0.05, R = 0.02, z = 0.6
    //   innovation = 0.6 - 0.5 = 0.1
    //   K = 0.05 / (0.05 + 0.02) = 0.05 / 0.07 = 0.7142857142857143
    //   x_post = 0.5 + 0.7142857142857143 * 0.1 = 0.5714285714285714
    //   P_post = (1 - 0.7142857142857143) * 0.05 = 0.014285714285714287
    val predicted = KalmanState(x = 0.5, p = 0.05)
    val result = AdaptiveKalmanFilter.update(predicted, observation = 0.6, measurementVariance = 0.02)

    assert(math.abs(result.innovation - 0.1) < 1e-12)
    assert(math.abs(result.kalmanGain - 0.7142857142857143) < 1e-12)
    assert(math.abs(result.posterior.x - 0.5714285714285714) < 1e-12)
    assert(math.abs(result.posterior.p - 0.014285714285714287) < 1e-12)
  }

  test("update rejects a non-positive measurement variance") {
    assertThrows[IllegalArgumentException] {
      AdaptiveKalmanFilter.update(KalmanState(0.5, 0.05), observation = 0.6, measurementVariance = 0.0)
    }
  }

  // -- 13. Posterior variance remains non-negative ---------------------------

  test("posterior variance is always non-negative, even with a very large measurement variance") {
    val predicted = KalmanState(x = 0.5, p = 0.05)
    val result = AdaptiveKalmanFilter.update(predicted, observation = 0.9, measurementVariance = 1000.0)
    assert(result.posterior.p >= 0.0)
  }

  // -- 3 & 4. Reliability -> measurement variance monotonicity -------------------

  test("higher reliability produces lower measurement variance") {
    val rLow = ReliabilityEstimator.reliabilityToMeasurementVariance(0.2, config)
    val rHigh = ReliabilityEstimator.reliabilityToMeasurementVariance(0.9, config)
    assert(rHigh < rLow, "a more reliable observation must get a SMALLER measurement variance")
  }

  test("lower reliability produces higher measurement variance") {
    val rHigh = ReliabilityEstimator.reliabilityToMeasurementVariance(1.0, config)
    val rLow = ReliabilityEstimator.reliabilityToMeasurementVariance(0.1, config)
    assert(rLow > rHigh, "a less reliable observation must get a LARGER measurement variance")
  }

  // -- 5. Adaptive mode == static mode when reliability = 1 -----------------------

  test("stepAdaptive with reliability=1.0 produces the identical measurement variance as stepStatic") {
    val prior = KalmanState(0.4, 0.1)
    val (_, staticResult) = AdaptiveKalmanFilter.stepStatic(prior, elapsedDays = 5, observation = 0.45, config)
    val (_, adaptiveResult) = AdaptiveKalmanFilter.stepAdaptive(prior, elapsedDays = 5, observation = 0.45, reliability = 1.0, config)
    assert(staticResult.posterior.x == adaptiveResult.posterior.x)
    assert(staticResult.posterior.p == adaptiveResult.posterior.p)
    assert(staticResult.kalmanGain == adaptiveResult.kalmanGain)
  }

  // -- 6. A low-reliability observation has LESS influence than the same observation at high reliability --

  test("the same observation influences the posterior less under low reliability than under high reliability") {
    val prior = KalmanState(0.4, 0.1)
    val surprisingObservation = 0.9 // far from the prior mean, to make the influence difference visible
    val (_, highRelResult) = AdaptiveKalmanFilter.stepAdaptive(prior, 5, surprisingObservation, reliability = 1.0, config)
    val (_, lowRelResult) = AdaptiveKalmanFilter.stepAdaptive(prior, 5, surprisingObservation, reliability = 0.1, config)

    val highRelMove = math.abs(highRelResult.posterior.x - prior.x)
    val lowRelMove = math.abs(lowRelResult.posterior.x - prior.x)
    assert(lowRelMove < highRelMove, "a low-reliability observation must move the posterior LESS than the same observation at high reliability")
    assert(highRelResult.kalmanGain > lowRelResult.kalmanGain, "the Kalman gain itself must be smaller under low reliability")
  }

  // -- Multi-source fusion: reduces exactly to single-source update ---------------

  test("updateMultiSource with exactly one observation matches update() exactly") {
    val predicted = KalmanState(x = 0.5, p = 0.05)
    val single = AdaptiveKalmanFilter.update(predicted, observation = 0.6, measurementVariance = 0.02)
    val multi = AdaptiveKalmanFilter.updateMultiSource(predicted, Seq(AdaptiveKalmanFilter.Observation(0.6, 0.02)))
    assert(math.abs(single.posterior.x - multi.posterior.x) < 1e-12)
    assert(math.abs(single.posterior.p - multi.posterior.p) < 1e-12)
  }

  test("updateMultiSource requires at least one observation") {
    assertThrows[IllegalArgumentException] {
      AdaptiveKalmanFilter.updateMultiSource(KalmanState(0.5, 0.05), Seq.empty)
    }
  }

  // -- 15. Small multi-source fixture: two sources incorporated without violating ordering --

  test("fusing two simultaneous observations weights the more reliable (lower-variance) source more heavily") {
    val predicted = KalmanState(x = 0.5, p = 0.05)
    // Source A: reliable (low variance), value far from prior. Source B: unreliable (high variance), value near prior.
    val sourceA = AdaptiveKalmanFilter.Observation(value = 0.9, measurementVariance = 0.001)
    val sourceB = AdaptiveKalmanFilter.Observation(value = 0.5, measurementVariance = 10.0)
    val result = AdaptiveKalmanFilter.updateMultiSource(predicted, Seq(sourceA, sourceB))
    // The fused observation should sit very close to sourceA's value, since it dominates by inverse-variance weighting.
    assert(math.abs(result.posterior.x - 0.9) < 0.05, s"fused posterior (${result.posterior.x}) should be dominated by the far more reliable source A")
  }
}
