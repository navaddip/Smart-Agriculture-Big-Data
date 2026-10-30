package smartagri.mapreduce

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.config.AppConfig
import smartagri.model.SentinelObservation

/**
 * Phase 3, Task 2A -- HDFS INPUT PREPARATION ONLY.
 *
 * Converts the existing, working Sentinel-2 NDVI Parquet table into a
 * deterministic, tab-delimited `TextInputFormat`-compatible dataset in
 * HDFS, for the future (NOT YET IMPLEMENTED) Hadoop MapReduce job
 * designed in Task 2's design report.
 *
 *     Parquet (processed/sentinel2_ndvi.parquet)
 *             |
 *             v
 *     Spark format conversion (THIS OBJECT -- selection + text formatting only)
 *             |
 *             v
 *     HDFS text input (processed/mapreduce/ndvi_flat_input/)
 *             |
 *             v
 *     future Hadoop MapReduce job (Task 2B -- NOT implemented here)
 *
 * IMPORTANT: Spark's role here stops at format preparation. This object
 * computes source-side validation counts (row count, missing-NDVI count,
 * invalid-NDVI count) purely to verify the export is complete and
 * lossless -- it does NOT compute the per-(tile, year-month) aggregation
 * statistics (mean/min/max/stddev/quality-rate) that Task 2's design
 * assigned to the future MapReduce Reducer. Computing those statistics
 * here would duplicate the very computation the MapReduce job exists to
 * demonstrate (Task 2 design report, section J: MapReduce vs Spark
 * responsibility boundary).
 */
object NdviFlatInputPreparation {

  /** The five columns this export selects, and no others -- matches the schema Task 2's design settled on. */
  val ExportColumns: Seq[String] = Seq("tile", "date", "cell_row", "cell_col", "ndvi")

  /**
   * Deterministic missing-NDVI sentinel written to the `ndvi` text column.
   *
   * DECISION (Task 2A requirement 5): "NA" rather than an empty string.
   * A trailing empty field before a newline is easy to lose or
   * mis-tokenize under a naive `line.split("\t")` in the future Mapper
   * (an empty trailing field can silently vanish depending on the split
   * implementation used), whereas "NA" is an explicit, unambiguous,
   * always-present token the Mapper can check for by exact string
   * equality. "NA" can never collide with a real NDVI value, since NDVI
   * is always numeric.
   *
   * This token is part of the output contract handed to the future
   * Mapper (Task 2B): treat this exact string as MISSING, and otherwise
   * parse the field as a `Double` and classify VALID/INVALID against
   * `SentinelObservation.NdviMin`/`NdviMax` -- the same canonical bounds
   * this object uses below for its own source-side validation, so the
   * definition of "invalid" is never independently re-decided in two
   * places.
   */
  val MissingNdviToken: String = "NA"

  /** Result of validating the source Parquet, before anything is written. */
  final case class SourceValidation(
      sourceRowCount: Long,
      missingNdviCount: Long,
      invalidNdviCount: Long,
      validNdviCount: Long
  )

  /** Result of the full export, including the source-side validation it was checked against. */
  final case class ExportResult(
      sourcePath: String,
      outputPath: String,
      validation: SourceValidation,
      exportedRecordCount: Long
  )

  /**
   * Verifies the source Parquet has every required column. Throws a
   * descriptive `IllegalStateException` naming exactly what's missing --
   * per Task 2A requirement 8, this must be checked BEFORE any read of
   * row-level data is trusted, and per this project's established
   * pattern (Task 1), a schema problem is reported with a useful error,
   * never silently worked around.
   */
  def validateSourceSchema(source: DataFrame): Unit = {
    val presentColumns = source.columns.toSet
    val missingColumns = ExportColumns.filterNot(presentColumns.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalStateException(
        s"Source Parquet is missing required column(s): ${missingColumns.mkString(", ")}. " +
          s"Present columns: ${source.columns.mkString(", ")}"
      )
    }
  }

  /**
   * Computes source-side validation counts ONLY (row count, missing NDVI,
   * invalid NDVI, valid NDVI) -- these are existence/completeness checks
   * on the source, not the MapReduce job's own statistics (see the class
   * doc comment). Does not modify the source DataFrame or its underlying
   * Parquet file in any way (Task 2A requirement 8: "Do NOT modify the
   * source Parquet" -- this function only ever reads).
   */
  def validateSource(source: DataFrame): SourceValidation = {
    validateSourceSchema(source)

    val sourceRowCount = source.count()

    val missingNdviCount = source.filter(col("ndvi").isNull).count()

    val invalidNdviCount = source
      .filter(
        col("ndvi").isNotNull &&
          (col("ndvi") < lit(SentinelObservation.NdviMin) || col("ndvi") > lit(SentinelObservation.NdviMax))
      )
      .count()

    val validNdviCount = sourceRowCount - missingNdviCount - invalidNdviCount

    SourceValidation(
      sourceRowCount = sourceRowCount,
      missingNdviCount = missingNdviCount,
      invalidNdviCount = invalidNdviCount,
      validNdviCount = validNdviCount
    )
  }

  /**
   * Builds the exact five-column export DataFrame, in a fixed column
   * order, with NDVI rendered as:
   *   - the deterministic `MissingNdviToken` when the source value is null
   *   - the plain numeric string otherwise, WHETHER OR NOT it falls
   *     within the valid range -- an out-of-range value is preserved
   *     verbatim, never clipped or dropped (Task 2A requirement 6). The
   *     future Mapper is responsible for classifying it as INVALID by
   *     comparing the parsed number against `SentinelObservation`'s
   *     canonical bounds; this method does not tag it in any special way
   *     beyond leaving it as a plain (if out-of-range) number, since no
   *     row-dropping or value-rewriting happens here regardless of
   *     validity.
   *
   * This is a pure, deterministic 1-to-1 row transformation -- no
   * filtering, no sampling, no aggregation -- so the row count in equals
   * the row count out (Task 2A requirement 7: determinism; requirement 2:
   * preserve the source row count).
   */
  def buildExportRows(source: DataFrame): DataFrame =
    source.select(
      col("tile"),
      col("date"),
      col("cell_row"),
      col("cell_col"),
      when(col("ndvi").isNull, lit(MissingNdviToken))
        .otherwise(col("ndvi").cast("string"))
        .as("ndvi")
    )

  /**
   * Runs the full export: validate source -> build export rows -> write
   * tab-delimited text to `outputPath` -> verify the written record count
   * matches the source row count exactly.
   *
   * HEADER DECISION (Task 2A requirement 4): NO header row is written
   * (`header` option left at its default `false`). Reasoning: Spark's
   * `.csv()` writer produces one output file per partition (a directory
   * of part-files is the normal, Hadoop-idiomatic form of a
   * `TextInputFormat` input -- deliberately NOT coalesced to a single
   * file, since forcing everything through one write task has no benefit
   * here and works against genuinely distributed practice). A header row
   * would then appear once per part-file, not once per logical dataset,
   * which would force the future Mapper to special-case "am I reading
   * the first line of *a* file" against a hard-coded header string on
   * every split -- fragile, and easy to get subtly wrong across multiple
   * files/splits. A headerless dataset has no such ambiguity: every line,
   * in every part-file, in any split, is a data record.
   *
   * PARTITIONING: rows are NOT explicitly repartitioned or coalesced
   * before writing. The resulting part-file count follows Spark's
   * existing partitioning of the source read, which is left as-is
   * because Task 2A's job is format preparation, not partition tuning --
   * partition/shuffle behavior is explicitly deferred to the later
   * performance-tuning task, not decided incidentally here.
   */
  def run(spark: SparkSession, sourcePath: String, outputPath: String): ExportResult = {
    val source = spark.read.parquet(sourcePath)

    val validation = validateSource(source)

    val exportRows = buildExportRows(source)

    exportRows.write
      .mode("overwrite")
      .option("sep", "\t")
      .option("header", value = false)
      .csv(outputPath)

    val exportedRecordCount = spark.read
      .option("sep", "\t")
      .option("header", value = false)
      .csv(outputPath)
      .count()

    if (exportedRecordCount != validation.sourceRowCount) {
      throw new IllegalStateException(
        s"Export row count ($exportedRecordCount) does not match source row count " +
          s"(${validation.sourceRowCount}) -- the export must be lossless. Aborting rather than " +
          s"leaving a silently incomplete dataset at $outputPath."
      )
    }

    ExportResult(
      sourcePath = sourcePath,
      outputPath = outputPath,
      validation = validation,
      exportedRecordCount = exportedRecordCount
    )
  }

  /** Default source/output paths, derived from the shared `AppConfig` (Task 1) rather than
    * re-hardcoded here -- consistent with every other module built since Task 1. */
  def defaultSourcePath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/sentinel2_ndvi.parquet"

  def defaultOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/mapreduce/ndvi_flat_input"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NdviFlatInputPreparation").getOrCreate()
    try {
      val config = AppConfig.default
      val result = run(spark, defaultSourcePath(config), defaultOutputPath(config))
      // scalastyle:off println
      println(s"NDVI flat input export complete: $result")
      // scalastyle:on println
    } finally {
      spark.stop()
    }
  }
}
