package smartagri.features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Filtering and splitting logic for the future-stress prediction
 * instances, per Task 4B sections C/H/K.
 */
object FeatureSplit {

  /** Approved minimum-history requirement (Task 4A revision, section H): a prediction instance at T
    * is only eligible if its cell has at least this many strictly-prior observations. */
  /** 10 is the approved value. SMART_AGRI_MIN_HISTORY overrides it ONLY for labelled sensitivity
    * experiments (results/experiments/...), never for the main results. */
  val MinimumPriorHistory: Int = sys.env.get("SMART_AGRI_MIN_HISTORY").map(_.toInt).getOrElse(10)

  /** Approved chronological split cutoff (Task 4A revision, section I). */
  val SplitCutoffDate: String = "2024-04-07"

  /**
   * Restricts to rows that have a valid future observation (i.e. are
   * NOT each cell's final observation) -- Task 4B section C: "The final
   * observation of each cell has no future target and must be excluded."
   * Must be called after `FeatureEngineering.withFuturePairing`.
   */
  def withFuturePairingOnly(df: DataFrame): DataFrame =
    df.filter(col("future_date").isNotNull && col("future_ndvi").isNotNull)

  /**
   * Applies the approved minimum-history filter (Task 4A section H,
   * revised from an earlier "&ge;5" to "&ge;10" based on measured
   * retention curves). Must be called after
   * `FeatureEngineering.withFrozenBaseline`.
   */
  def withMinimumHistory(df: DataFrame): DataFrame =
    df.filter(col("baseline_observation_count") >= MinimumPriorHistory)

  final case class SplitResult(train: DataFrame, test: DataFrame, straddling: DataFrame)

  /**
   * Chronological split (Task 4B section K) -- NEVER a random row split.
   *
   *   TRAIN:      date < cutoff  AND  future_date < cutoff
   *   TEST:       date >= cutoff AND  future_date >= cutoff
   *   STRADDLING: date < cutoff  AND  future_date >= cutoff  (excluded from both)
   *
   * The fourth logical combination (`date >= cutoff AND future_date < cutoff`)
   * is impossible by construction -- `future_date` is always chronologically
   * after `date` for the same cell, so it can never be earlier than a
   * `date` that is itself already at/after the cutoff. These three
   * categories are therefore exhaustive and mutually exclusive over any
   * input that has already been through `withFuturePairingOnly`.
   */
  def chronologicalSplit(df: DataFrame, cutoff: String = SplitCutoffDate): SplitResult = {
    val train = df.filter(col("date") < cutoff && col("future_date") < cutoff)
    val test = df.filter(col("date") >= cutoff && col("future_date") >= cutoff)
    val straddling = df.filter(col("date") < cutoff && col("future_date") >= cutoff)
    SplitResult(train, test, straddling)
  }
}
