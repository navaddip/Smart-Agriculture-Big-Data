package smartagri.conformal

/**
 * Split conformal prediction for the Task 10 binary crop-stress
 * classifier -- the minimum viable method Task 11 asks for, not full
 * multiclass/adaptive conformal machinery.
 *
 * METHOD (documented explicitly, per Task 11's requirement not to claim
 * guarantees beyond what's implemented):
 *
 *   1. A CALIBRATION set, held out from model fitting entirely (never
 *      used to fit the classifier), with known labels.
 *   2. For each calibration example, the nonconformity score is
 *      `1 - P(true label)`, i.e. how much probability mass the model
 *      placed on the WRONG outcome.
 *   3. `qhat` = the ceil((n+1)(1-alpha))/n empirical quantile of those
 *      calibration scores -- the standard finite-sample-corrected split
 *      conformal quantile (Vovk et al.), not a naive `(1-alpha)`
 *      quantile, which would systematically under-cover.
 *   4. For a TEST example, a label `y` is included in the prediction
 *      set if `1 - P(y) <= qhat`.
 *
 * GUARANTEE, STATED PRECISELY: under the standard conformal exchangeability
 * assumption (calibration and test points are exchangeable), this method
 * gives marginal coverage >= 1 - alpha over the test set AS A WHOLE, in
 * expectation over draws of the calibration set -- NOT a per-example or
 * conditional guarantee, and not a claim that THIS SPECIFIC calibration
 * set's coverage will hit that target exactly. Because this project's
 * data is temporally ordered rather than exchangeable in the classical
 * i.i.d. sense, this guarantee is presented as the METHOD's textbook
 * property, not independently re-derived or verified for this specific
 * temporal setting.
 */
object ConformalPrediction {

  final case class CalibrationExample(sampleId: String, trueLabel: Int, probPositive: Double)
  final case class TestExample(sampleId: String, trueLabel: Int, probPositive: Double)

  final case class PredictionSet(sampleId: String, trueLabel: Int, includesNegative: Boolean, includesPositive: Boolean) {
    def size: Int = (if (includesNegative) 1 else 0) + (if (includesPositive) 1 else 0)
    def containsTrueLabel: Boolean = if (trueLabel == 1) includesPositive else includesNegative
  }

  private def nonconformityScore(trueLabel: Int, probPositive: Double): Double = {
    val probTrueLabel = if (trueLabel == 1) probPositive else (1.0 - probPositive)
    1.0 - probTrueLabel
  }

  /**
   * Computes `qhat` from calibration examples ONLY -- callers must never
   * pass test-set labels here (Task 11's absolute rule: "No test labels
   * may be used during calibration").
   */
  def calibrate(calibration: Seq[CalibrationExample], alpha: Double): Double = {
    require(calibration.nonEmpty, "calibration set must not be empty")
    require(alpha > 0.0 && alpha < 1.0, s"alpha must be in (0,1), was $alpha")

    val scores = calibration.map(c => nonconformityScore(c.trueLabel, c.probPositive)).sorted
    val n = scores.length
    val rank = math.ceil((n + 1) * (1.0 - alpha)).toInt
    if (rank > n) 1.0 else scores(rank - 1)
  }

  /** Builds the prediction set for one test example, given a FROZEN `qhat` (from `calibrate`,
    * computed once on calibration data only, then applied unchanged to every test example). */
  def predictionSet(test: TestExample, qhat: Double): PredictionSet = {
    val includesNegative = nonconformityScore(0, test.probPositive) <= qhat
    val includesPositive = nonconformityScore(1, test.probPositive) <= qhat
    PredictionSet(test.sampleId, test.trueLabel, includesNegative, includesPositive)
  }

  def predictionSets(tests: Seq[TestExample], qhat: Double): Seq[PredictionSet] =
    tests.map(predictionSet(_, qhat))

  final case class CoverageReport(empiricalCoverage: Double, averageSetSize: Double, count: Long, ambiguousCount: Long)

  /** Empirical coverage: the fraction of TEST examples whose actual true label was inside the
    * predicted set -- computed directly from what actually happened, not assumed from theory. */
  def evaluateCoverage(sets: Seq[PredictionSet]): CoverageReport = {
    val n = sets.length
    val covered = sets.count(_.containsTrueLabel)
    val totalSetSize = sets.map(_.size).sum
    val ambiguous = sets.count(_.size == 2)
    CoverageReport(
      empiricalCoverage = if (n > 0) covered.toDouble / n else Double.NaN,
      averageSetSize = if (n > 0) totalSetSize.toDouble / n else Double.NaN,
      count = n.toLong,
      ambiguousCount = ambiguous.toLong
    )
  }
}
