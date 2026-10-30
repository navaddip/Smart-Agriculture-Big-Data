package smartagri.mapreduce

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import smartagri.model.SentinelObservation

/**
 * Focused tests for `NdviFlatInputPreparation`'s export/validation logic
 * (Task 2A requirement 12).
 *
 * These use a local, in-process `SparkSession` (`master("local[*]")`) --
 * this is the standard, legitimate way to unit-test Spark transformation
 * logic, not a "fake Hadoop integration test": no HDFS, no YARN, and no
 * real cluster are involved or pretended to be involved. `run()`'s HDFS
 * write/read-back is exercised here against the LOCAL filesystem (a
 * temp directory), which is sufficient to test the export/validation
 * logic itself; it is not a substitute for the real HDFS run required by
 * Task 2D, and this suite makes no claim that it is.
 */
class NdviFlatInputPreparationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("NdviFlatInputPreparationSpec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private val schema = StructType(
    Seq(
      StructField("tile", StringType, nullable = false),
      StructField("date", StringType, nullable = false),
      StructField("cell_row", IntegerType, nullable = false),
      StructField("cell_col", IntegerType, nullable = false),
      StructField("lat", DoubleType, nullable = false),
      StructField("lon", DoubleType, nullable = false),
      StructField("ndvi", FloatType, nullable = true)
    )
  )

  private def sourceDf(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  private val validRow = Row("T43PHQ", "2023-04-13", 0, 0, 13.55, 77.78, 0.42f)
  private val missingNdviRow = Row("T43PHQ", "2023-04-13", 0, 1, 13.55, 77.79, null)
  private val invalidHighRow = Row("T43PHQ", "2023-04-13", 0, 2, 13.55, 77.80, 1.5f)
  private val invalidLowRow = Row("T43PHQ", "2023-04-13", 0, 3, 13.55, 77.81, -2.0f)

  // -- Schema validation -----------------------------------------------

  test("validateSourceSchema does not throw when a DataFrame with all five required columns is passed") {
    val df = sourceDf(Seq(validRow))
    NdviFlatInputPreparation.validateSourceSchema(df) // should not throw
  }

  // -- Source validation -------------------------------------------------

  test("validateSource counts rows, missing NDVI, invalid NDVI, and valid NDVI correctly") {
    val df = sourceDf(Seq(validRow, missingNdviRow, invalidHighRow, invalidLowRow))
    val result = NdviFlatInputPreparation.validateSource(df)
    assert(result.sourceRowCount == 4)
    assert(result.missingNdviCount == 1)
    assert(result.invalidNdviCount == 2)
    assert(result.validNdviCount == 1)
  }

  test("validateSource reports zero missing/invalid when all NDVI values are valid") {
    val df = sourceDf(Seq(validRow, validRow.copy()))
    val result = NdviFlatInputPreparation.validateSource(df)
    assert(result.missingNdviCount == 0)
    assert(result.invalidNdviCount == 0)
  }

  test("validateSource uses the same canonical NDVI bounds as SentinelObservation (boundary values are valid)") {
    val exactlyMin = Row("T43PHQ", "2023-04-13", 0, 0, 13.55, 77.78, SentinelObservation.NdviMin.toFloat)
    val exactlyMax = Row("T43PHQ", "2023-04-13", 0, 1, 13.55, 77.79, SentinelObservation.NdviMax.toFloat)
    val df = sourceDf(Seq(exactlyMin, exactlyMax))
    val result = NdviFlatInputPreparation.validateSource(df)
    assert(result.invalidNdviCount == 0)
    assert(result.validNdviCount == 2)
  }

  test("validateSourceSchema throws a descriptive error when a required column is missing") {
    val incompleteSchema = StructType(schema.filterNot(_.name == "ndvi"))
    import scala.collection.JavaConverters._
    val df = spark.createDataFrame(
      Seq(Row("T43PHQ", "2023-04-13", 0, 0, 13.55, 77.78)).asJava,
      incompleteSchema
    )
    val ex = intercept[IllegalStateException] {
      NdviFlatInputPreparation.validateSourceSchema(df)
    }
    assert(ex.getMessage.contains("ndvi"))
  }

  // -- Export row building ------------------------------------------------

  test("buildExportRows preserves row count exactly (no filtering, no dropping)") {
    val df = sourceDf(Seq(validRow, missingNdviRow, invalidHighRow, invalidLowRow))
    val exported = NdviFlatInputPreparation.buildExportRows(df)
    assert(exported.count() == 4)
  }

  test("buildExportRows selects exactly the five intended columns, in order") {
    val df = sourceDf(Seq(validRow))
    val exported = NdviFlatInputPreparation.buildExportRows(df)
    assert(exported.columns.toSeq == Seq("tile", "date", "cell_row", "cell_col", "ndvi"))
  }

  test("buildExportRows renders a missing NDVI as the deterministic MissingNdviToken, not blank or dropped") {
    val df = sourceDf(Seq(missingNdviRow))
    val exported = NdviFlatInputPreparation.buildExportRows(df)
    val ndviValue = exported.collect()(0).getAs[String]("ndvi")
    assert(ndviValue == NdviFlatInputPreparation.MissingNdviToken)
    assert(ndviValue == "NA")
  }

  test("buildExportRows preserves an out-of-range NDVI value verbatim rather than clipping or dropping it") {
    val df = sourceDf(Seq(invalidHighRow, invalidLowRow))
    val exported = NdviFlatInputPreparation.buildExportRows(df)
    val values = exported.collect().map(_.getAs[String]("ndvi")).toSet
    assert(values.contains("1.5"))
    assert(values.contains("-2.0"))
  }

  test("buildExportRows renders a valid NDVI as its plain numeric string") {
    val df = sourceDf(Seq(validRow))
    val exported = NdviFlatInputPreparation.buildExportRows(df)
    val ndviValue = exported.collect()(0).getAs[String]("ndvi")
    assert(ndviValue == "0.42")
  }

  // -- Determinism ---------------------------------------------------------

  test("buildExportRows is deterministic: running it twice on the same input yields the same rows") {
    val df = sourceDf(Seq(validRow, missingNdviRow, invalidHighRow))
    val first = NdviFlatInputPreparation.buildExportRows(df).collect().map(_.mkString("\t")).sorted
    val second = NdviFlatInputPreparation.buildExportRows(df).collect().map(_.mkString("\t")).sorted
    assert(first.toSeq == second.toSeq)
  }

  // -- End-to-end run() against the local filesystem (not real HDFS) -------

  test("run() writes a headerless, tab-delimited dataset whose row count matches the source, to a local path") {
    val df = sourceDf(Seq(validRow, missingNdviRow, invalidHighRow, invalidLowRow))
    val tempSourcePath = java.nio.file.Files.createTempDirectory("ndvi_source").toString
    val tempOutputPath = java.nio.file.Files.createTempDirectory("ndvi_output").toString
    java.nio.file.Files.delete(java.nio.file.Paths.get(tempOutputPath)) // let Spark create it fresh

    df.write.mode("overwrite").parquet(tempSourcePath)

    val result = NdviFlatInputPreparation.run(spark, tempSourcePath, tempOutputPath)

    assert(result.validation.sourceRowCount == 4)
    assert(result.exportedRecordCount == 4)

    // Confirm no header line: every written line must be a genuine data record (5 tab-separated fields).
    val writtenLines = spark.read.textFile(tempOutputPath).collect()
    assert(writtenLines.length == 4)
    writtenLines.foreach { line =>
      assert(line.split("\t", -1).length == 5, s"expected 5 tab-separated fields, got: $line")
    }
  }

  test("run() throws rather than silently succeeding if the source is missing a required column") {
    val incompleteSchema = StructType(schema.filterNot(_.name == "ndvi"))
    import scala.collection.JavaConverters._
    val df = spark.createDataFrame(
      Seq(Row("T43PHQ", "2023-04-13", 0, 0, 13.55, 77.78)).asJava,
      incompleteSchema
    )
    val tempSourcePath = java.nio.file.Files.createTempDirectory("ndvi_bad_source").toString
    val tempOutputPath = java.nio.file.Files.createTempDirectory("ndvi_bad_output").toString
    java.nio.file.Files.delete(java.nio.file.Paths.get(tempOutputPath))

    df.write.mode("overwrite").parquet(tempSourcePath)

    intercept[IllegalStateException] {
      NdviFlatInputPreparation.run(spark, tempSourcePath, tempOutputPath)
    }
  }
}
