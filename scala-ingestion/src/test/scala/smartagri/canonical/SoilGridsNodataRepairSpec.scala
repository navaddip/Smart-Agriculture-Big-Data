package smartagri.canonical

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SoilGridsNodataRepairSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("SoilGridsNodataRepairSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val schema = StructType(Seq(
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

  private def df(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  private val allZeroRow = Row("T43PHQ", 0, 20, 13.5, 77.5, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
  private val realRow = Row("T43PHQ", 0, 21, 13.5, 77.6, 6.5, 1.2, 15.0, 30.0, 40.0, 20.0, 25.0)

  // -- 3. SoilGrids all-seven-zero -> missing rule ------------------------

  test("a row where all seven soil properties are exactly 0.0 is converted to null across all seven") {
    val source = df(Seq(allZeroRow))
    val (repaired, result) = SoilGridsNodataRepair.repair(source)

    assert(result.originalRows == 1)
    assert(result.affectedRows == 1)
    val row = repaired.collect()(0)
    SoilGridsNodataRepair.PropertyColumns.foreach { c =>
      assert(row.isNullAt(row.fieldIndex(c)), s"$c should be null after repair")
    }
    assert(row.getAs[String]("tile") == "T43PHQ")
    assert(row.getAs[Int]("cell_row") == 0)
  }

  test("original row count is preserved exactly -- repair nulls values, never drops rows") {
    val source = df(Seq(allZeroRow, realRow))
    val (repaired, result) = SoilGridsNodataRepair.repair(source)
    assert(result.originalRows == 2)
    assert(result.finalRows == 2)
    assert(repaired.count() == 2)
  }

  // -- 4. SoilGrids genuine zero values NOT globally converted -------------

  test("a row where only SOME properties are zero (not all seven) is left completely untouched") {
    val partialZeroRow = Row("T43PHQ", 0, 22, 13.5, 77.7, 0.0, 1.2, 15.0, 30.0, 40.0, 20.0, 25.0)
    val source = df(Seq(partialZeroRow))
    val (repaired, result) = SoilGridsNodataRepair.repair(source)

    assert(result.affectedRows == 0, "a partial-zero row must NOT be flagged as nodata")
    val row = repaired.collect()(0)
    assert(!row.isNullAt(row.fieldIndex("ph_water")), "ph_water must remain 0.0, not become null")
    assert(row.getAs[Double]("ph_water") == 0.0)
    assert(row.getAs[Double]("nitrogen_g_kg") == 1.2)
  }

  test("a row with genuinely realistic (all non-zero) values is completely unaffected") {
    val source = df(Seq(realRow))
    val (repaired, result) = SoilGridsNodataRepair.repair(source)
    assert(result.affectedRows == 0)
    val row = repaired.collect()(0)
    assert(row.getAs[Double]("ph_water") == 6.5)
    assert(row.getAs[Double]("water_content_fc") == 25.0)
  }

  test("a mix of nodata and real rows: only the all-zero row is repaired, the real row is untouched") {
    val source = df(Seq(allZeroRow, realRow))
    val (repaired, result) = SoilGridsNodataRepair.repair(source)
    assert(result.affectedRows == 1)

    val rows = repaired.collect()
    val allZeroResult = rows.find(_.getAs[Int]("cell_col") == 20).get
    val realResult = rows.find(_.getAs[Int]("cell_col") == 21).get

    assert(allZeroResult.isNullAt(allZeroResult.fieldIndex("ph_water")))
    assert(!realResult.isNullAt(realResult.fieldIndex("ph_water")))
    assert(realResult.getAs[Double]("ph_water") == 6.5)
  }
}
