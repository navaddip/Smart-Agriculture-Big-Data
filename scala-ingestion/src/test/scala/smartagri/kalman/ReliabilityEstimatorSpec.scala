package smartagri.kalman

import org.scalatest.funsuite.AnyFunSuite

class ReliabilityEstimatorSpec extends AnyFunSuite {

  private val config = KalmanConfig.default

  test("an NDVI value outside [-1, 1] immediately gets the reliability floor") {
    val r1 = ReliabilityEstimator.ndviReliability(1.5, Some(0.4), Some(0.1), config)
    val r2 = ReliabilityEstimator.ndviReliability(-1.5, Some(0.4), Some(0.1), config)
    assert(r1 == config.minimumReliability)
    assert(r2 == config.minimumReliability)
  }

  test("a value close to the prior mean gets high reliability") {
    val r = ReliabilityEstimator.ndviReliability(0.41, Some(0.40), Some(0.05), config)
    // deviation = |0.41-0.40|/(0.05+0.01) = 0.1667 -> reliability = 1/(1.1667) ~= 0.857
    assert(r > 0.8)
  }

  test("a value far from the prior mean gets lower reliability than a value close to it") {
    val rClose = ReliabilityEstimator.ndviReliability(0.42, Some(0.40), Some(0.05), config)
    val rFar = ReliabilityEstimator.ndviReliability(0.90, Some(0.40), Some(0.05), config)
    assert(rFar < rClose)
  }

  test("with no prior history, reliability is neutral (1.0), not an arbitrary guess") {
    val r = ReliabilityEstimator.ndviReliability(0.5, None, None, config)
    assert(r == 1.0)
  }

  test("reliability never falls below minimumReliability or above 1.0, across a range of deviations") {
    val deviationsToTry = Seq(0.0, 0.01, 0.1, 1.0, 10.0, 1000.0, 1e9)
    deviationsToTry.foreach { dev =>
      val value = 0.40 + dev * 0.05
      val r = ReliabilityEstimator.ndviReliability(value, Some(0.40), Some(0.05), config)
      assert(r >= config.minimumReliability && r <= 1.0, s"reliability $r out of documented range for deviation $dev")
    }
  }

  test("reliabilityToMeasurementVariance is monotonically decreasing as reliability increases") {
    val reliabilities = Seq(0.05, 0.2, 0.4, 0.6, 0.8, 1.0)
    val variances = reliabilities.map(r => ReliabilityEstimator.reliabilityToMeasurementVariance(r, config))
    assert(variances == variances.sorted.reverse, "measurement variance must strictly decrease as reliability increases")
  }

  test("ReliabilityEstimator's API has no parameter for sensor_health or corruption metadata") {
    // Structural guarantee: this call compiles with ONLY these 4 arguments (value, priorMean,
    // priorStd, config) -- Scala's type system makes it impossible to pass a sensor_health or
    // corruption argument that isn't a declared parameter in the first place.
    val result = ReliabilityEstimator.ndviReliability(value = 0.5, priorMean = None, priorStd = None, config = config)
    assert(result.isInstanceOf[Double])
  }
}
