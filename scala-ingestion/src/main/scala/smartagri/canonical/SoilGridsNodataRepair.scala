package smartagri.canonical

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Fixes the SoilGrids hidden-nodata defect found in the Task 3A audit:
 * 8,522 of 19,926 rows (42.8%) have all seven soil properties
 * simultaneously equal to exactly `0.0`, believed to be out-of-coverage
 * pixels whose intended zero-to-null masking (`extract_soilgrids.py`)
 * was skipped because it only ran inside an `if nodata is not None`
 * guard -- see that file's narrow fix, applied separately in Python for
 * future re-extractions.
 */
object SoilGridsNodataRepair {

  /** The seven soil property columns this rule applies to, in the canonical order. */
  val PropertyColumns: Seq[String] = Seq(
    "ph_water", "nitrogen_g_kg", "soil_organic_carbon",
    "clay_pct", "sand_pct", "silt_pct", "water_content_fc"
  )

  final case class RepairResult(
      originalRows: Long,
      affectedRows: Long,
      finalRows: Long
  )

  /**
   * Converts the seven soil property columns to null, but ONLY for rows
   * where ALL SEVEN are simultaneously exactly `0.0` -- the specific
   * nodata signature identified in Task 3A. A row where only some
   * properties happen to be zero is left completely untouched (Task 3B's
   * explicit instruction: "Do NOT convert every individual zero into
   * null").
   *
   * IMPLEMENTATION NOTE: the all-zero flag is computed ONCE from the
   * original column values and materialized as its own boolean column
   * (`_all_zero_nodata`) BEFORE any of the seven property columns are
   * rewritten. This matters: if the all-zero condition were instead
   * re-evaluated fresh for each of the seven `withColumn` calls in a
   * left-to-right fold, the second and later calls would be comparing
   * against columns already nulled by the first call -- `null === 0.0`
   * evaluates to `null` (not `true`), which would silently break the
   * condition partway through and leave later columns un-repaired. The
   * one-time materialized flag avoids that entirely.
   */
  def repair(source: DataFrame): (DataFrame, RepairResult) = {
    val originalRows = source.count()

    val allZeroExpr = PropertyColumns.map(c => col(c) === 0.0).reduce(_ && _)
    val flagged = source.withColumn("_all_zero_nodata", allZeroExpr)

    val affectedRows = flagged.filter(col("_all_zero_nodata")).count()

    val withNullsApplied = PropertyColumns.foldLeft(flagged) { (df, propertyColumn) =>
      df.withColumn(
        propertyColumn,
        when(col("_all_zero_nodata"), lit(null).cast("double")).otherwise(col(propertyColumn))
      )
    }

    val repaired = withNullsApplied.drop("_all_zero_nodata")
    val finalRows = repaired.count()

    (repaired, RepairResult(originalRows, affectedRows, finalRows))
  }
}
