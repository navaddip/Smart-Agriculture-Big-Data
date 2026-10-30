package smartagri.mapreduce

/**
 * Pure, Hadoop-independent, single-pass, O(1)-memory aggregator for one
 * `(tile, year_month)` group's NDVI observations.
 *
 * Isolated here specifically so the aggregation arithmetic (mean/stddev/
 * quality-rate, and the `validCount == 0` / `validCount == 1` edge
 * cases) can be unit-tested directly, without a Hadoop `Reducer.Context`
 * or a live MapReduce run. `NdviTileMonthReducer` folds its
 * `Iterable[NdviObservationWritable]` through exactly one instance of
 * this class in a single pass -- never buffering the values via
 * `.toList`/`.toSeq`/`.toArray`/`collect()` (Task 2B's explicit
 * streaming-reduce requirement).
 *
 * Deliberately mutable (`accumulate` mutates internal state rather than
 * returning a new instance) -- this is an accumulator meant to be folded
 * over a large, one-pass `Iterator` without allocating a new object per
 * observation, which matters at real data volume even though it departs
 * from this project's usual immutable style (Task 1's `GridCell`,
 * `CanonicalDate`, etc.) for the same practical reason `NdviObservationWritable`
 * does.
 */
final class NdviTileMonthAggregator {
  private var observationCount: Long = 0L
  private var validCount: Long = 0L
  private var invalidCount: Long = 0L
  private var missingCount: Long = 0L
  private var sum: Double = 0.0d
  private var sumOfSquares: Double = 0.0d
  private var minValue: Double = Double.PositiveInfinity
  private var maxValue: Double = Double.NegativeInfinity

  /** Folds one observation into the running aggregate state. O(1) time and space per call. */
  def accumulate(observation: NdviObservationWritable): this.type = {
    observationCount += 1
    observation.status match {
      case NdviStatus.Missing =>
        missingCount += 1
      case NdviStatus.Valid =>
        validCount += 1
        sum += observation.value
        sumOfSquares += observation.value * observation.value
        if (observation.value < minValue) minValue = observation.value
        if (observation.value > maxValue) maxValue = observation.value
      case NdviStatus.Invalid =>
        invalidCount += 1
      case unknown =>
        throw new IllegalStateException(s"Unrecognized NdviObservationWritable status byte: $unknown")
    }
    this
  }

  /**
   * Computes the final summary from whatever has been accumulated so
   * far. Safe to call at any point; typically called once, after the
   * Reducer's single pass over its `Iterable` completes.
   */
  def summary(): NdviTileMonthAggregator.Summary = {
    require(
      validCount + invalidCount + missingCount == observationCount,
      s"internal invariant violated: validCount($validCount) + invalidCount($invalidCount) + " +
        s"missingCount($missingCount) != observationCount($observationCount)"
    )

    val meanNdvi: Option[Double] = if (validCount > 0) Some(sum / validCount) else None

    val stddevNdvi: Option[Double] =
      if (validCount >= 2) {
        val mean = sum / validCount
        // POPULATION variance (divide by validCount, not validCount - 1), intentionally, unchanged
        // from Task 2B's original specification (confirmed during the Task 2C audit, not silently
        // altered): this statistic describes the actual, complete set of valid NDVI observations
        // this tile received in this month -- not a sample drawn from some larger population this
        // group is meant to estimate -- so the population formula is the correct one, not an
        // approximation standing in for the sample formula.
        val rawVariance = sumOfSquares / validCount - mean * mean
        // Protect against a tiny negative value from floating-point error (Task 2B requirement) --
        // mathematically variance can never be negative, but sum-of-squares-minus-mean-squared can
        // land fractionally below zero due to floating-point rounding when the true variance is ~0.
        val variance = if (rawVariance < 0.0d) 0.0d else rawVariance
        Some(math.sqrt(variance))
      } else {
        None
      }

    val minNdvi: Option[Double] = if (validCount > 0) Some(minValue) else None
    val maxNdvi: Option[Double] = if (validCount > 0) Some(maxValue) else None

    val qualityRate: Double = if (observationCount > 0) validCount.toDouble / observationCount else 0.0d

    NdviTileMonthAggregator.Summary(
      observationCount = observationCount,
      validCount = validCount,
      invalidCount = invalidCount,
      missingCount = missingCount,
      meanNdvi = meanNdvi,
      minNdvi = minNdvi,
      maxNdvi = maxNdvi,
      stddevNdvi = stddevNdvi,
      qualityRate = qualityRate
    )
  }
}

object NdviTileMonthAggregator {

  /** Explicit sentinel for an unavailable mean/min/max/stddev (`validCount == 0`, or `validCount < 2`
    * specifically for stddev) -- NEVER a silently-emitted `0.0`, per Task 2B's explicit requirement. */
  val UnavailableToken: String = "NA"

  final case class Summary(
      observationCount: Long,
      validCount: Long,
      invalidCount: Long,
      missingCount: Long,
      meanNdvi: Option[Double],
      minNdvi: Option[Double],
      maxNdvi: Option[Double],
      stddevNdvi: Option[Double],
      qualityRate: Double
  ) {
    private def render(v: Option[Double]): String = v.map(_.toString).getOrElse(UnavailableToken)

    /**
     * The nine statistic fields, tab-delimited, in the exact order Task
     * 2B specifies AFTER `tile` and `year_month` (which the caller
     * prepends, since this summary only knows the statistics, not which
     * group it belongs to): `observation_count, valid_count,
     * invalid_count, missing_count, mean_ndvi, min_ndvi, max_ndvi,
     * stddev_ndvi, quality_rate`.
     */
    def toTabDelimitedFields: Seq[String] = Seq(
      observationCount.toString,
      validCount.toString,
      invalidCount.toString,
      missingCount.toString,
      render(meanNdvi),
      render(minNdvi),
      render(maxNdvi),
      render(stddevNdvi),
      qualityRate.toString
    )
  }
}
