package smartagri.canonical

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class Era5SpatialAlignmentSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("Era5SpatialAlignmentSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val sentinelSchema = StructType(Seq(
    StructField("tile", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("cell_row", IntegerType, nullable = false),
    StructField("cell_col", IntegerType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("ndvi", DoubleType, nullable = false)
  ))

  private val era5Schema = StructType(Seq(
    StructField("date", StringType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("swvl1", DoubleType, nullable = false)
  ))

  private def sentinelDf(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, sentinelSchema)
  }
  private def era5Df(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, era5Schema)
  }

  test("each Sentinel row is attached to the nearest ERA5 point's weather value for the same date") {
    val sentinel = sentinelDf(Seq(
      Row("T43PHQ", "2023-04-13", 0, 0, 13.0, 77.1, 0.4)
    ))
    val era5 = era5Df(Seq(
      Row("2023-04-13", 13.0, 77.0, 0.25),
      Row("2023-04-13", 14.0, 78.0, 0.99)
    ))
    val (joined, result) = Era5SpatialAlignment.align(spark, sentinel, era5)
    assert(result.sentinelRowsBefore == 1)
    assert(result.matchedRows == 1)
    assert(result.unmatchedRows == 0)
    assert(result.era5PointsUsed == 2)

    val row = joined.collect()(0)
    assert(row.getAs[Double]("swvl1") == 0.25)
    assert(row.getAs[Double]("era5_source_lat") == 13.0)
    assert(row.getAs[Double]("era5_source_lon") == 77.0)
  }

  test("row count is preserved exactly through the ERA5 join -- no explosion, no loss") {
    val sentinel = sentinelDf(Seq(
      Row("T43PHQ", "2023-04-13", 0, 0, 13.0, 77.0, 0.4),
      Row("T43PHQ", "2023-04-13", 0, 1, 13.0, 77.0, 0.5),
      Row("T43PHQ", "2023-04-13", 0, 2, 13.0, 77.0, 0.6)
    ))
    val era5 = era5Df(Seq(Row("2023-04-13", 13.0, 77.0, 0.25)))
    val (joined, result) = Era5SpatialAlignment.align(spark, sentinel, era5)
    assert(joined.count() == 3)
    assert(result.sentinelRowsBefore == 3)
    assert(result.matchedRows == 3)
  }

  test("a Sentinel date with no matching ERA5 date is reported as unmatched, not silently dropped") {
    val sentinel = sentinelDf(Seq(
      Row("T43PHQ", "2099-01-01", 0, 0, 13.0, 77.0, 0.4)
    ))
    val era5 = era5Df(Seq(Row("2023-04-13", 13.0, 77.0, 0.25)))
    val (joined, result) = Era5SpatialAlignment.align(spark, sentinel, era5)

    assert(result.unmatchedRows == 1)
    assert(result.matchedRows == 0)
    assert(result.unmatchedDates == Seq("2099-01-01"))
    assert(joined.count() == 1)
    val row = joined.collect()(0)
    assert(row.isNullAt(row.fieldIndex("swvl1")))
  }

  test("a Sentinel row is never matched to an ERA5 observation from a different date, even if geographically closer") {
    val sentinel = sentinelDf(Seq(
      Row("T43PHQ", "2023-04-13", 0, 0, 13.0, 77.0, 0.4)
    ))
    val era5 = era5Df(Seq(
      Row("2023-04-12", 13.0, 77.0, 0.11),
      Row("2023-04-13", 13.0, 77.0, 0.25)
    ))
    val (joined, _) = Era5SpatialAlignment.align(spark, sentinel, era5)
    val row = joined.collect()(0)
    assert(row.getAs[Double]("swvl1") == 0.25, "must match the same-date ERA5 value, never a different date's")
  }
}
