package smartagri.canonical

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Fixes the Sentinel duplicate-block defect found in the Task 3A audit:
 * 19,840 rows (9,920 duplicate `(tile, date, cell_row, cell_col)` keys),
 * entirely confined to `T43PGQ / 2023-04-23`, where the extraction
 * pipeline wrote the same grid-cell block twice.
 */
object SentinelDeduplication {

  val CanonicalKeyColumns: Seq[String] = Seq("tile", "date", "cell_row", "cell_col")

  final case class DedupResult(
      originalRows: Long,
      duplicateKeyCount: Long,
      duplicateRowsRemoved: Long,
      finalRows: Long
  )

  /**
   * Verifies that every duplicate-key group is byte-identical across
   * `lat`, `lon`, and `ndvi` -- i.e. a true, safe-to-collapse duplicate,
   * not two genuinely different observations that happen to share a key.
   *
   * Throws `IllegalStateException` if ANY group disagrees (Task 3B's
   * explicit instruction: "STOP and report it rather than inventing a
   * resolution rule"). This function never picks a winner among
   * conflicting values -- it only ever confirms there is nothing to
   * choose between.
   */
  def verifyDuplicatesAreIdentical(df: DataFrame): Unit = {
    val grouped = df
      .groupBy(CanonicalKeyColumns.map(col): _*)
      .agg(
        countDistinct(struct(col("lat"), col("lon"), col("ndvi"))).as("distinct_value_combos"),
        count(lit(1)).as("row_count")
      )
      .filter(col("row_count") > 1)

    val conflictingCount = grouped.filter(col("distinct_value_combos") > 1).count()

    if (conflictingCount > 0) {
      throw new IllegalStateException(
        s"Sentinel deduplication aborted: found $conflictingCount duplicate-key group(s) with " +
          "CONFLICTING lat/lon/ndvi values across rows sharing the same (tile, date, cell_row, " +
          "cell_col). These are not simple exact duplicates and must not be silently resolved by " +
          "picking one -- inspect them manually before proceeding."
      )
    }
  }

  /**
   * Deduplicates by the exact canonical Sentinel identity, retaining one
   * copy of each duplicate group. Only ever called after
   * `verifyDuplicatesAreIdentical` has confirmed there is nothing to
   * lose by doing so -- no aggregation, no averaging, per Task 3B's
   * explicit instruction.
   */
  def deduplicate(source: DataFrame): (DataFrame, DedupResult) = {
    val originalRows = source.count()

    verifyDuplicatesAreIdentical(source)

    val keyCounts = source.groupBy(CanonicalKeyColumns.map(col): _*).count()
    val duplicateKeyCount = keyCounts.filter(col("count") > 1).count()

    val deduped = source.dropDuplicates(CanonicalKeyColumns)
    val finalRows = deduped.count()
    val duplicateRowsRemoved = originalRows - finalRows

    (deduped, DedupResult(originalRows, duplicateKeyCount, duplicateRowsRemoved, finalRows))
  }
}
