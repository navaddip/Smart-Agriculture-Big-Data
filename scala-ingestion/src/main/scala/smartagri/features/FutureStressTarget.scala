package smartagri.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Constructs the approved future-stress target (Task 4A revision, Task
 * 4B section E): `future_anomaly` from the frozen baseline and
 * `future_ndvi`, and `future_stressed` from a threshold chosen using
 * TRAINING data only.
 */
object FutureStressTarget {

  /**
   * Adds `future_anomaly = (future_ndvi - baseline_mean_ndvi) / baseline_std_ndvi`.
   *
   * Rows where `baseline_std_ndvi` is null or exactly `0.0` get a null
   * `future_anomaly` rather than `Infinity`/`NaN` from a division by
   * zero -- a constant-history baseline (every prior observation
   * identical) makes the z-score undefined, not automatically "extreme,"
   * and this must be surfaced as missing, never silently computed as an
   * enormous or infinite number.
   */
  def withFutureAnomaly(df: DataFrame): DataFrame =
    df.withColumn(
      "future_anomaly",
      when(col("baseline_std_ndvi").isNotNull && col("baseline_std_ndvi") =!= 0.0,
        (col("future_ndvi") - col("baseline_mean_ndvi")) / col("baseline_std_ndvi")
      )
    )

  final case class AnomalyDistribution(
      count: Long,
      mean: Double,
      stddev: Double,
      min: Double,
      max: Double,
      p10: Double,
      p25: Double,
      p50: Double,
      p75: Double,
      p90: Double
  )

  /** Reports the real `future_anomaly` distribution -- computed, never assumed, per Task 4B section E. */
  def anomalyDistribution(df: DataFrame): AnomalyDistribution = {
    val valid = df.filter(col("future_anomaly").isNotNull)
    val stats = valid.agg(
      count(col("future_anomaly")),
      mean(col("future_anomaly")),
      stddev(col("future_anomaly")),
      min(col("future_anomaly")),
      max(col("future_anomaly"))
    ).head()
    val percentiles = valid.stat.approxQuantile("future_anomaly", Array(0.10, 0.25, 0.50, 0.75, 0.90), 0.001)

    AnomalyDistribution(
      count = stats.getLong(0),
      mean = stats.getDouble(1),
      stddev = stats.getDouble(2),
      min = stats.getDouble(3),
      max = stats.getDouble(4),
      p10 = percentiles(0),
      p25 = percentiles(1),
      p50 = percentiles(2),
      p75 = percentiles(3),
      p90 = percentiles(4)
    )
  }

  final case class ThresholdClassBalance(threshold: Double, positiveCount: Long, negativeCount: Long, positiveRate: Double)

  /** Reports class balance at each candidate threshold, computed ONLY from the DataFrame passed in --
    * callers must pass the TRAINING partition only (Task 4B section E/L: threshold selection must
    * never see test data). This function has no knowledge of splits; it trusts the caller. */
  def classBalanceAtThresholds(df: DataFrame, candidateThresholds: Seq[Double]): Seq[ThresholdClassBalance] = {
    val valid = df.filter(col("future_anomaly").isNotNull)
    val total = valid.count()
    candidateThresholds.map { t =>
      val positives = valid.filter(col("future_anomaly") < t).count()
      val negatives = total - positives
      ThresholdClassBalance(t, positives, negatives, if (total > 0) positives.toDouble / total else 0.0)
    }
  }

  /**
   * Applies a FROZEN threshold (chosen upstream from training data only,
   * per Task 4B section E/L) to produce `future_stressed`. Rows with a
   * null `future_anomaly` get a null `future_stressed`, not a default
   * `0` -- an undefined anomaly must not silently become "not stressed."
   */
  def withFutureStressed(df: DataFrame, threshold: Double): DataFrame =
    df.withColumn(
      "future_stressed",
      when(col("future_anomaly").isNotNull, when(col("future_anomaly") < threshold, 1).otherwise(0))
    )

  /**
   * APPROVED, FROZEN THRESHOLD (Task 4B section E).
   *
   * Selected by inspecting the real training-partition `future_anomaly`
   * distribution (109,328 rows, chronological split at 2024-04-07,
   * train side only) and the resulting class balance at each of the
   * four candidates the task specified:
   *
   *   threshold=-0.5: 93.8% positive / 6.2% negative  -- severely imbalanced
   *   threshold=-1.0: 56.2% positive / 43.8% negative -- well balanced
   *   threshold=-1.5: 13.2% positive / 86.8% negative -- imbalanced the other way
   *   threshold=-2.0:  3.2% positive / 96.8% negative -- severely imbalanced
   *
   * -1.0 is chosen because it is, by a wide margin, the best-balanced of
   * the four candidates (56/44, versus the next-closest at 13/87) AND
   * because it has a standard, defensible statistical interpretation
   * ("more than one standard deviation below this cell's own historical
   * mean") -- not chosen to maximize any accuracy metric, and not chosen
   * using any test-partition data.
   */
  val ApprovedThreshold: Double = -1.0
}
