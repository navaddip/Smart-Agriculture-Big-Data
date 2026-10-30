package smartagri.features

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Leakage-safe feature engineering over the canonical historical dataset
 * (Task 3B's output), per Task 4B section C/D/G.
 *
 * Every function here operates via Spark `Window` functions ordered by
 * `date` within each `(tile, cell_row, cell_col)` partition -- this is
 * what makes "strictly before T" / "strictly after T" a precise,
 * enforceable frame boundary rather than a hope. `rowsBetween` bounds
 * are chosen explicitly and documented at each call site; nothing here
 * uses an unbounded or symmetric window that could reach into the
 * future.
 */
object FeatureEngineering {

  /** How many strictly-prior observations the short-term rolling NDVI statistics look back over. */
  val RollingWindowSize: Int = 4

  private def cellOrderedByDate: org.apache.spark.sql.expressions.WindowSpec =
    Window.partitionBy("tile", "cell_row", "cell_col").orderBy("date")

  /**
   * Adds `future_date` and `future_ndvi` (Task 4B section C) via `lead(_, 1)`
   * over the per-cell, date-ordered window -- the next chronological
   * Sentinel observation for the SAME cell, never a calendar-day
   * assumption. A row whose cell has no later observation gets `null`
   * in both columns and must be filtered out by the caller (its final
   * observation has no valid target) -- this function does not filter;
   * it only computes the columns, so the filtering decision stays
   * visible and separately testable (see `FeatureSplit.withFuturePairingOnly`).
   */
  def withFuturePairing(canonical: DataFrame): DataFrame = {
    val w = cellOrderedByDate
    canonical
      .withColumn("future_date", lead(col("date"), 1).over(w))
      .withColumn("future_ndvi", lead(col("ndvi"), 1).over(w))
  }

  /**
   * Adds `ndvi_lag1`, `ndvi_change`, short-term rolling NDVI mean/std
   * (over the `RollingWindowSize` most recent STRICTLY PRIOR
   * observations, never including the current row), and
   * `days_since_prior_observation` (Task 4B section G "temporal
   * satellite" features).
   *
   * The rolling window uses `rowsBetween(-RollingWindowSize, -1)` --
   * explicitly excluding row offset 0 (the current row) and everything
   * after it. This is the exact distinction Task 4A's design flagged:
   * `rowsBetween(-4, -1)` is valid for a prediction at T; `rowsBetween(-2, 2)`
   * would not be.
   */
  def withLagAndRollingFeatures(df: DataFrame): DataFrame = {
    val w = cellOrderedByDate
    val rollingWindow = w.rowsBetween(-RollingWindowSize, -1)

    df
      .withColumn("ndvi_lag1", lag(col("ndvi"), 1).over(w))
      .withColumn("ndvi_change", col("ndvi") - col("ndvi_lag1"))
      .withColumn("ndvi_rolling_mean", avg(col("ndvi")).over(rollingWindow))
      .withColumn("ndvi_rolling_std", stddev(col("ndvi")).over(rollingWindow))
      .withColumn("prior_date", lag(col("date"), 1).over(w))
      .withColumn(
        "days_since_prior_observation",
        when(col("prior_date").isNotNull, datediff(to_date(col("date")), to_date(col("prior_date"))))
      )
      .drop("prior_date")
  }

  /**
   * Adds the frozen expanding baseline (Task 4B section D):
   * `baseline_mean_ndvi`, `baseline_std_ndvi`, `baseline_observation_count`,
   * computed over ALL observations strictly before the current row for
   * the same cell -- `rowsBetween(Window.unboundedPreceding, -1)`. Offset
   * `-1` is the critical bound: it excludes offset `0` (the current row,
   * i.e. `NDVI(T)`), so the baseline can never include the very
   * observation it is meant to be a baseline FOR, let alone anything
   * later. `count` uses the same frame, so `baseline_observation_count`
   * is exactly the number of rows the mean/std were computed from --
   * useful both for the minimum-history filter (`FeatureSplit`) and for
   * downstream audit.
   */
  def withFrozenBaseline(df: DataFrame): DataFrame = {
    val w = cellOrderedByDate
    val expandingPriorWindow = w.rowsBetween(Window.unboundedPreceding, -1)

    df
      .withColumn("baseline_mean_ndvi", avg(col("ndvi")).over(expandingPriorWindow))
      .withColumn("baseline_std_ndvi", stddev(col("ndvi")).over(expandingPriorWindow))
      .withColumn("baseline_observation_count", count(col("ndvi")).over(expandingPriorWindow))
  }

  /** Cyclical day-of-year encoding (Task 4B section G) -- sin/cos so December 31 and January 1 are
    * numerically close, matching the actual seasonal continuity, rather than 365 apart. */
  def withCyclicalDateFeatures(df: DataFrame): DataFrame = {
    val dayOfYear = dayofyear(to_date(col("date"))).cast("double")
    val twoPi = 2.0 * math.Pi
    df
      .withColumn("day_of_year_sin", sin(dayOfYear / lit(365.25) * lit(twoPi)))
      .withColumn("day_of_year_cos", cos(dayOfYear / lit(365.25) * lit(twoPi)))
  }

  /** Applies all leakage-safe engineered-feature steps in one call, in the order that makes each
    * subsequent step's inputs available (future pairing and lag/rolling/baseline are independent of
    * each other and could be reordered freely; grouped here purely for a single entry point). */
  def engineerAll(canonical: DataFrame): DataFrame = {
    val withFuture = withFuturePairing(canonical)
    val withLagRolling = withLagAndRollingFeatures(withFuture)
    val withBaseline = withFrozenBaseline(withLagRolling)
    withCyclicalDateFeatures(withBaseline)
  }
}
