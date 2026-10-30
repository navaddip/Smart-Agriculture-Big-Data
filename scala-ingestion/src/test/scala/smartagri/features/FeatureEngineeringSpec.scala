package smartagri.features

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FeatureEngineeringSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("FeatureEngineeringSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val schema = StructType(Seq(
    StructField("tile", StringType, false), StructField("cell_row", IntegerType, false),
    StructField("cell_col", IntegerType, false), StructField("date", StringType, false),
    StructField("ndvi", DoubleType, true)
  ))

  private def df(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  // A single cell with 6 hand-chosen NDVI values on 6 known dates.
  private lazy val sixObs = df(Seq(
    Row("T1", 0, 0, "2024-01-01", 0.10),
    Row("T1", 0, 0, "2024-01-06", 0.20),
    Row("T1", 0, 0, "2024-01-11", 0.30),
    Row("T1", 0, 0, "2024-01-16", 0.40),
    Row("T1", 0, 0, "2024-01-21", 0.50),
    Row("T1", 0, 0, "2024-01-26", 0.60)
  ))

  // -- 1. T->T+1 pairing --------------------------------------------------

  test("future_date/future_ndvi correctly hold the NEXT chronological observation for the same cell") {
    val result = FeatureEngineering.withFuturePairing(sixObs).orderBy("date").collect()
    assert(result(0).getAs[String]("future_date") == "2024-01-06")
    assert(result(0).getAs[Double]("future_ndvi") == 0.20)
    assert(result(2).getAs[String]("future_date") == "2024-01-16")
    assert(result(2).getAs[Double]("future_ndvi") == 0.40)
  }

  // -- 2. Last observation has no target -----------------------------------

  test("the final observation of a cell has null future_date and future_ndvi") {
    val result = FeatureEngineering.withFuturePairing(sixObs).orderBy("date").collect()
    val last = result.last
    assert(last.isNullAt(last.fieldIndex("future_date")))
    assert(last.isNullAt(last.fieldIndex("future_ndvi")))
  }

  // -- 3. Lag correctness ---------------------------------------------------

  test("ndvi_lag1 holds exactly the PREVIOUS observation's NDVI, and ndvi_change is the correct delta") {
    val result = FeatureEngineering.withLagAndRollingFeatures(sixObs).orderBy("date").collect()
    assert(result(0).isNullAt(result(0).fieldIndex("ndvi_lag1")), "first observation has no prior")
    assert(result(1).getAs[Double]("ndvi_lag1") == 0.10)
    assert(math.abs(result(1).getAs[Double]("ndvi_change") - 0.10) < 1e-9)
    assert(result(3).getAs[Double]("ndvi_lag1") == 0.30)
  }

  // -- 4. Rolling-window correctness (strictly prior only) -----------------

  test("ndvi_rolling_mean at the 5th observation uses exactly the 4 prior values (0.10,0.20,0.30,0.40), never the current 0.50") {
    val result = FeatureEngineering.withLagAndRollingFeatures(sixObs).orderBy("date").collect()
    val fifthRow = result(4) // date 2024-01-21, ndvi=0.50
    val expectedMean = (0.10 + 0.20 + 0.30 + 0.40) / 4.0
    assert(math.abs(fifthRow.getAs[Double]("ndvi_rolling_mean") - expectedMean) < 1e-9,
      "rolling mean must be computed from the 4 prior rows only, excluding the current row's own 0.50")
  }

  test("ndvi_rolling_mean never includes a future value: the 6th row's rolling window is entirely from rows 2-5") {
    val result = FeatureEngineering.withLagAndRollingFeatures(sixObs).orderBy("date").collect()
    val sixthRow = result(5) // date 2024-01-26, ndvi=0.60 -- this value must NOT appear in its own rolling stat
    val expectedMean = (0.20 + 0.30 + 0.40 + 0.50) / 4.0
    assert(math.abs(sixthRow.getAs[Double]("ndvi_rolling_mean") - expectedMean) < 1e-9)
  }

  test("days_since_prior_observation matches the actual calendar gap, null for the first observation") {
    val result = FeatureEngineering.withLagAndRollingFeatures(sixObs).orderBy("date").collect()
    assert(result(0).isNullAt(result(0).fieldIndex("days_since_prior_observation")))
    assert(result(1).getAs[Int]("days_since_prior_observation") == 5)
  }

  // -- 5. Baseline excludes current T; 6. Baseline excludes future T+1 -------

  test("baseline_mean_ndvi/std at the 4th observation use ONLY the 3 strictly-prior observations, never the current one") {
    val result = FeatureEngineering.withFrozenBaseline(sixObs).orderBy("date").collect()
    val fourthRow = result(3) // date 2024-01-16, ndvi=0.40 -- must NOT be included in its own baseline
    val priorValues = Seq(0.10, 0.20, 0.30)
    val expectedMean = priorValues.sum / priorValues.size
    assert(math.abs(fourthRow.getAs[Double]("baseline_mean_ndvi") - expectedMean) < 1e-9)
    assert(fourthRow.getAs[Long]("baseline_observation_count") == 3L)
  }

  test("baseline_observation_count grows by exactly 1 per observation and never counts the current or any future row") {
    val result = FeatureEngineering.withFrozenBaseline(sixObs).orderBy("date").collect()
    assert(result(0).isNullAt(result(0).fieldIndex("baseline_mean_ndvi")), "first observation has zero prior history")
    (1 until 6).foreach { i =>
      assert(result(i).getAs[Long]("baseline_observation_count") == i.toLong,
        s"row $i should have exactly $i prior observations backing its baseline")
    }
  }

  test("baseline is frozen at T: appending a later observation does not change an earlier row's baseline") {
    val fiveObs = df(Seq(
      Row("T1", 0, 0, "2024-01-01", 0.10),
      Row("T1", 0, 0, "2024-01-06", 0.20),
      Row("T1", 0, 0, "2024-01-11", 0.30),
      Row("T1", 0, 0, "2024-01-16", 0.40)
    ))
    val baselineBefore = FeatureEngineering.withFrozenBaseline(fiveObs).orderBy("date").collect()(3)
      .getAs[Double]("baseline_mean_ndvi")
    val baselineAfter = FeatureEngineering.withFrozenBaseline(sixObs).orderBy("date").collect()(3)
      .getAs[Double]("baseline_mean_ndvi")
    assert(math.abs(baselineBefore - baselineAfter) < 1e-9,
      "the 4th row's baseline must be identical whether or not a 5th/6th observation exists later")
  }
}
