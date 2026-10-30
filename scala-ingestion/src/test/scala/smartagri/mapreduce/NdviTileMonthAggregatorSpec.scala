package smartagri.mapreduce

import org.scalatest.funsuite.AnyFunSuite

class NdviTileMonthAggregatorSpec extends AnyFunSuite {

  // -- custom minimal tolerance helper (avoids depending on ScalaTest's Matchers trait) --
  private case class DoubleTolerance(value: Double, tolerance: Double)
  private implicit class ToleranceOps(d: Double) {
    def +-(tol: Double): DoubleTolerance = DoubleTolerance(d, tol)
  }
  private implicit class OptionDoubleContains(opt: Option[Double]) {
    def containsApprox(t: DoubleTolerance): Boolean = opt.exists(v => math.abs(v - t.value) <= t.tolerance)
  }

  // -- 9. Basic reducer aggregation -----------------------------------------

  test("accumulating three valid observations produces correct count/sum-derived stats") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.2))
    agg.accumulate(NdviObservationWritable.valid(0.4))
    agg.accumulate(NdviObservationWritable.valid(0.6))
    val s = agg.summary()

    assert(s.observationCount == 3)
    assert(s.validCount == 3)
    assert(s.invalidCount == 0)
    assert(s.missingCount == 0)
    assert(s.meanNdvi.containsApprox(0.4 +- 1e-9))
    assert(s.minNdvi.contains(0.2))
    assert(s.maxNdvi.contains(0.6))
  }

  // -- 10. Mixed missing + invalid + valid ------------------------------------

  test("accumulating a mix of missing, invalid, and valid observations counts each bucket correctly") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.3))
    agg.accumulate(NdviObservationWritable.valid(0.5))
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.invalid(1.7))
    agg.accumulate(NdviObservationWritable.invalid(-1.7))
    val s = agg.summary()

    assert(s.observationCount == 5)
    assert(s.validCount == 2)
    assert(s.invalidCount == 2)
    assert(s.missingCount == 1)
    // Core correctness invariant Task 2B requires:
    assert(s.validCount + s.invalidCount + s.missingCount == s.observationCount)
    assert(s.meanNdvi.containsApprox(0.4 +- 1e-9))
  }

  test("invalid observations' values do not influence mean/min/max/stddev at all") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.5))
    agg.accumulate(NdviObservationWritable.invalid(999.0)) // wildly out of range
    val s = agg.summary()
    assert(s.meanNdvi.containsApprox(0.5 +- 1e-12))
    assert(s.minNdvi.containsApprox(0.5 +- 1e-12))
    assert(s.maxNdvi.containsApprox(0.5 +- 1e-12))
  }

  // -- 11. Zero valid observations ------------------------------------------------

  test("zero valid observations: mean/min/max/stddev are explicitly unavailable, never a silent 0.0") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.invalid(1.5))
    val s = agg.summary()

    assert(s.validCount == 0)
    assert(s.meanNdvi.isEmpty)
    assert(s.minNdvi.isEmpty)
    assert(s.maxNdvi.isEmpty)
    assert(s.stddevNdvi.isEmpty)
    // The rendered text output must show the explicit sentinel for mean/min/max/stddev
    // specifically -- never a silently-computed 0.0 for those four fields. (quality_rate,
    // the 9th field, legitimately CAN be "0.0" here -- 0 valid out of 2 observations really is
    // a 0.0 quality rate, which is meaningful, correct output, not a defaulted placeholder --
    // so it is deliberately excluded from this check.)
    val fields = s.toTabDelimitedFields
    val meanMinMaxStddevFields = fields.slice(4, 8) // mean_ndvi, min_ndvi, max_ndvi, stddev_ndvi
    assert(meanMinMaxStddevFields.forall(_ == NdviTileMonthAggregator.UnavailableToken))
    assert(!meanMinMaxStddevFields.contains("0.0"))
  }

  test("empty group (no observations at all) does not throw and reports zero/unavailable throughout") {
    val agg = new NdviTileMonthAggregator()
    val s = agg.summary()
    assert(s.observationCount == 0)
    assert(s.qualityRate == 0.0)
    assert(s.meanNdvi.isEmpty)
  }

  test("all-missing group: every count/stat field behaves like the zero-valid case") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.missing())
    val s = agg.summary()
    assert(s.observationCount == 3)
    assert(s.validCount == 0)
    assert(s.invalidCount == 0)
    assert(s.missingCount == 3)
    assert(s.validCount + s.invalidCount + s.missingCount == s.observationCount)
    assert(s.meanNdvi.isEmpty)
    assert(s.minNdvi.isEmpty)
    assert(s.maxNdvi.isEmpty)
    assert(s.stddevNdvi.isEmpty)
    assert(s.qualityRate == 0.0)
  }

  test("accumulating a VALID observation at exactly the canonical lower bound (-1.0) counts as valid") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(smartagri.model.SentinelObservation.NdviMin))
    val s = agg.summary()
    assert(s.validCount == 1)
    assert(s.minNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMin +- 1e-12))
    assert(s.maxNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMin +- 1e-12))
  }

  test("accumulating a VALID observation at exactly the canonical upper bound (1.0) counts as valid") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(smartagri.model.SentinelObservation.NdviMax))
    val s = agg.summary()
    assert(s.validCount == 1)
    assert(s.minNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMax +- 1e-12))
    assert(s.maxNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMax +- 1e-12))
  }

  test("min/max correctly span the full range when both boundary values are present alongside a mid value") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(smartagri.model.SentinelObservation.NdviMin))
    agg.accumulate(NdviObservationWritable.valid(0.0))
    agg.accumulate(NdviObservationWritable.valid(smartagri.model.SentinelObservation.NdviMax))
    val s = agg.summary()
    assert(s.minNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMin +- 1e-12))
    assert(s.maxNdvi.containsApprox(smartagri.model.SentinelObservation.NdviMax +- 1e-12))
  }

  // -- 12. Standard deviation calculation -----------------------------------------

  test("stddev is None (not 0.0) when there is exactly one valid observation") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.5))
    val s = agg.summary()
    assert(s.validCount == 1)
    assert(s.stddevNdvi.isEmpty, "a single sample has no meaningful variance and must not report 0.0")
  }

  test("stddev matches a hand-computed value for a small known set of valid observations") {
    // values: 0.2, 0.4, 0.6 -> mean = 0.4
    // population variance = mean((x-mean)^2) = ((-0.2)^2 + 0^2 + 0.2^2) / 3 = (0.04+0+0.04)/3 = 0.026666...
    // stddev = sqrt(0.026666...) ~= 0.16329931619
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.2))
    agg.accumulate(NdviObservationWritable.valid(0.4))
    agg.accumulate(NdviObservationWritable.valid(0.6))
    val s = agg.summary()
    val expected = 0.16329931619
    s.stddevNdvi match {
      case Some(actual) => assert(math.abs(actual - expected) < 1e-6)
      case None => fail("expected a computed stddev for 3 valid observations")
    }
  }

  test("stddev is exactly 0.0 (not NaN, not negative) when all valid observations are identical") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.5))
    agg.accumulate(NdviObservationWritable.valid(0.5))
    agg.accumulate(NdviObservationWritable.valid(0.5))
    val s = agg.summary()
    s.stddevNdvi match {
      case Some(actual) =>
        assert(!actual.isNaN, "stddev must not be NaN even when floating-point error pushes variance slightly negative")
        assert(actual == 0.0)
      case None => fail("expected stddev == 0.0 for identical observations, got None")
    }
  }

  // -- 13. Quality rate calculation ------------------------------------------------

  test("qualityRate is validCount / observationCount") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.5))
    agg.accumulate(NdviObservationWritable.valid(0.6))
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.invalid(2.0))
    val s = agg.summary()
    // 2 valid out of 4 total
    assert(math.abs(s.qualityRate - 0.5) < 1e-9)
  }

  test("qualityRate is 1.0 when every observation is valid") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.1))
    agg.accumulate(NdviObservationWritable.valid(0.2))
    val s = agg.summary()
    assert(s.qualityRate == 1.0)
  }

  test("qualityRate is 0.0 when no observation is valid") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.invalid(5.0))
    val s = agg.summary()
    assert(s.qualityRate == 0.0)
  }

  // -- Output field formatting -------------------------------------------------------

  test("toTabDelimitedFields produces exactly nine fields, in the specified order") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.4))
    val fields = agg.summary().toTabDelimitedFields
    assert(fields.length == 9)
  }

  // -- Correctness invariant, checked directly against the summary case class -------------

  test("summary() itself enforces validCount + invalidCount + missingCount == observationCount") {
    val agg = new NdviTileMonthAggregator()
    agg.accumulate(NdviObservationWritable.valid(0.1))
    agg.accumulate(NdviObservationWritable.missing())
    agg.accumulate(NdviObservationWritable.invalid(3.0))
    val s = agg.summary()
    assert(s.validCount + s.invalidCount + s.missingCount == s.observationCount)
  }
}
