package smartagri.canonical

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class CanonicalHistoricalDatasetBuilderSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("CanonicalHistoricalDatasetBuilderSpec").master("local[2]")
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
    StructField("swvl1", DoubleType, nullable = false),
    StructField("swvl2", DoubleType, nullable = false),
    StructField("swvl3", DoubleType, nullable = false),
    StructField("swvl4", DoubleType, nullable = false),
    StructField("u10", DoubleType, nullable = false),
    StructField("v10", DoubleType, nullable = false),
    StructField("t2m", DoubleType, nullable = false),
    StructField("d2m", DoubleType, nullable = false),
    StructField("ssrd", DoubleType, nullable = false),
    StructField("e", DoubleType, nullable = false),
    StructField("tp", DoubleType, nullable = false),
    StructField("pev", DoubleType, nullable = false)
  ))
  private val soilSchema = StructType(Seq(
    StructField("tile", StringType, nullable = false),
    StructField("cell_row", IntegerType, nullable = false),
    StructField("cell_col", IntegerType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("ph_water", DoubleType, nullable = true),
    StructField("nitrogen_g_kg", DoubleType, nullable = true),
    StructField("soil_organic_carbon", DoubleType, nullable = true),
    StructField("clay_pct", DoubleType, nullable = true),
    StructField("sand_pct", DoubleType, nullable = true),
    StructField("silt_pct", DoubleType, nullable = true),
    StructField("water_content_fc", DoubleType, nullable = true)
  ))

  private def mk(schema: StructType, rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  private def era5Row(date: String, lat: Double, lon: Double, v: Double) =
    Row(date, lat, lon, v, v, v, v, v, v, v, v, v, v, v, v)

  // -- 8. canonical-key uniqueness; 9. row-count preservation through joins ----

  test("canonical dataset has zero duplicate keys and row count equals deduplicated Sentinel row count") {
    val sentinel = mk(sentinelSchema, Seq(
      Row("T43PGQ", "2023-04-23", 0, 6, 13.0, 77.0, 0.34),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.0, 77.0, 0.34), // exact duplicate, per Task 3A's finding shape
      Row("T43PGQ", "2023-04-23", 0, 7, 13.0, 77.1, 0.31)
    ))
    val era5 = mk(era5Schema, Seq(era5Row("2023-04-23", 13.0, 77.0, 0.2)))
    val soil = mk(soilSchema, Seq(
      Row("T43PGQ", 0, 6, 13.0, 77.0, 6.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0),
      Row("T43PGQ", 0, 7, 13.0, 77.1, 6.1, 1.1, 10.1, 20.1, 30.1, 10.1, 15.1)
    ))

    val (canonical, result) = CanonicalHistoricalDatasetBuilder.build(spark, sentinel, era5, soil)

    assert(result.sentinelDedup.duplicateRowsRemoved == 1)
    assert(result.canonicalRowCount == 2, "one duplicate must be collapsed away")
    assert(result.distinctKeyCount == result.canonicalRowCount)
    assert(canonical.dropDuplicates(Seq("tile", "cell_row", "cell_col", "date")).count() == canonical.count())
  }

  test("canonical row count exactly equals deduplicated Sentinel row count (no join-induced explosion or loss)") {
    val sentinel = mk(sentinelSchema, (0 until 5).map(i =>
      Row("T43PHQ", "2023-04-13", 0, i, 13.0, 77.0 + i * 0.01, 0.4)))
    val era5 = mk(era5Schema, Seq(era5Row("2023-04-13", 13.0, 77.0, 0.2)))
    val soil = mk(soilSchema, (0 until 5).map(i =>
      Row("T43PHQ", 0, i, 13.0, 77.0 + i * 0.01, 6.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)))

    val (canonical, result) = CanonicalHistoricalDatasetBuilder.build(spark, sentinel, era5, soil)
    assert(canonical.count() == 5)
    assert(result.canonicalRowCount == result.sentinelDedup.finalRows)
  }

  test("the canonical schema contains exactly the required columns, in the specified order") {
    val sentinel = mk(sentinelSchema, Seq(Row("T43PHQ", "2023-04-13", 0, 0, 13.0, 77.0, 0.4)))
    val era5 = mk(era5Schema, Seq(era5Row("2023-04-13", 13.0, 77.0, 0.2)))
    val soil = mk(soilSchema, Seq(Row("T43PHQ", 0, 0, 13.0, 77.0, 6.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)))

    val (canonical, _) = CanonicalHistoricalDatasetBuilder.build(spark, sentinel, era5, soil)
    assert(canonical.columns.toSeq == CanonicalHistoricalDatasetBuilder.RequiredColumns :+ "weather_available")
  }

  test("validate() reports no failures against a correctly-built canonical dataset") {
    val sentinel = mk(sentinelSchema, Seq(Row("T43PHQ", "2023-04-13", 0, 0, 13.0, 77.0, 0.4)))
    val era5 = mk(era5Schema, Seq(era5Row("2023-04-13", 13.0, 77.0, 0.2)))
    val soil = mk(soilSchema, Seq(Row("T43PHQ", 0, 0, 13.0, 77.0, 6.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)))

    val (soilRepaired, _) = SoilGridsNodataRepair.repair(soil)
    val (canonical, result) = CanonicalHistoricalDatasetBuilder.build(spark, sentinel, era5, soil)
    val failures = CanonicalHistoricalDatasetBuilder.validate(canonical, result, soilRepaired)
    assert(failures.isEmpty, s"expected no validation failures, got: $failures")
  }
}
