package smartagri.features

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FutureStressTargetAndSplitSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("FutureStressTargetAndSplitSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val schema = StructType(Seq(
    StructField("tile", StringType, false), StructField("date", StringType, false),
    StructField("future_date", StringType, true),
    StructField("future_ndvi", DoubleType, true),
    StructField("baseline_mean_ndvi", DoubleType, true),
    StructField("baseline_std_ndvi", DoubleType, true),
    StructField("baseline_observation_count", LongType, true)
  ))
  private def df(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  // -- 7. future target uses T+1 NDVI ---------------------------------------

  test("future_anomaly = (future_ndvi - baseline_mean_ndvi) / baseline_std_ndvi, computed by hand") {
    val row = df(Seq(Row("T1", "2024-01-01", "2024-01-06", 0.30, 0.20, 0.05, 10L)))
    val result = FutureStressTarget.withFutureAnomaly(row).collect()(0)
    val expected = (0.30 - 0.20) / 0.05
    assert(math.abs(result.getAs[Double]("future_anomaly") - expected) < 1e-9)
  }

  test("future_anomaly is null (not Infinity/NaN) when baseline_std_ndvi is exactly 0.0") {
    val row = df(Seq(Row("T1", "2024-01-01", "2024-01-06", 0.30, 0.20, 0.0, 10L)))
    val result = FutureStressTarget.withFutureAnomaly(row).collect()(0)
    assert(result.isNullAt(result.fieldIndex("future_anomaly")))
  }

  test("future_stressed is 1 below the frozen threshold, 0 at/above it, and null when anomaly is null") {
    val rows = df(Seq(
      Row("T1", "2024-01-01", "2024-01-06", 0.30, 0.20, 0.05, 10L),  // anomaly = 2.0
      Row("T1", "2024-01-06", "2024-01-11", 0.10, 0.20, 0.05, 10L),  // anomaly = -2.0
      Row("T1", "2024-01-11", "2024-01-16", 0.20, 0.20, 0.0, 10L)    // std=0 -> anomaly null
    ))
    val withAnomaly = FutureStressTarget.withFutureAnomaly(rows)
    val result = FutureStressTarget.withFutureStressed(withAnomaly, threshold = -1.0).orderBy("date").collect()
    assert(result(0).getAs[Int]("future_stressed") == 0, "anomaly=2.0 is not below threshold -1.0")
    assert(result(1).getAs[Int]("future_stressed") == 1, "anomaly=-2.0 is below threshold -1.0")
    assert(result(2).isNullAt(result(2).fieldIndex("future_stressed")), "undefined anomaly must yield a null target, never a default 0")
  }

  // -- 9. Minimum-history filtering ------------------------------------------

  test("withMinimumHistory keeps only rows with baseline_observation_count >= 10") {
    val rows = df(Seq(
      Row("T1", "2024-01-01", "2024-01-06", 0.30, 0.20, 0.05, 9L),
      Row("T1", "2024-01-06", "2024-01-11", 0.30, 0.20, 0.05, 10L),
      Row("T1", "2024-01-11", "2024-01-16", 0.30, 0.20, 0.05, 11L)
    ))
    val result = FeatureSplit.withMinimumHistory(rows)
    assert(result.count() == 2)
  }

  // -- 10. Chronological split; 11. Straddling-pair exclusion -----------------

  test("chronologicalSplit correctly assigns train/test/straddling and excludes straddling from both") {
    val rows = df(Seq(
      Row("T1", "2024-04-01", "2024-04-06", 0.30, 0.20, 0.05, 10L), // both < cutoff -> TRAIN
      Row("T1", "2024-04-05", "2024-04-10", 0.30, 0.20, 0.05, 10L), // date<cutoff, future>=cutoff -> STRADDLING
      Row("T1", "2024-04-07", "2024-04-12", 0.30, 0.20, 0.05, 10L), // both >= cutoff -> TEST
      Row("T1", "2024-04-10", "2024-04-15", 0.30, 0.20, 0.05, 10L)  // both >= cutoff -> TEST
    ))
    val split = FeatureSplit.chronologicalSplit(rows, cutoff = "2024-04-07")
    assert(split.train.count() == 1)
    assert(split.train.collect()(0).getAs[String]("date") == "2024-04-01")
    assert(split.test.count() == 2)
    assert(split.straddling.count() == 1)
    assert(split.straddling.collect()(0).getAs[String]("date") == "2024-04-05")
    // exhaustiveness: every input row appears in exactly one category
    assert(split.train.count() + split.test.count() + split.straddling.count() == rows.count())
  }

  test("a straddling row never appears in train or test output") {
    val rows = df(Seq(Row("T1", "2024-04-05", "2024-04-10", 0.30, 0.20, 0.05, 10L)))
    val split = FeatureSplit.chronologicalSplit(rows, cutoff = "2024-04-07")
    assert(split.train.count() == 0)
    assert(split.test.count() == 0)
    assert(split.straddling.count() == 1)
  }

  // -- 13 & 14. canonical-key preservation / no duplicate prediction instances --

  test("withFuturePairingOnly removes rows with no future observation, keeping canonical keys unique") {
    val schema2 = StructType(Seq(
      StructField("tile", StringType, false), StructField("cell_row", IntegerType, false),
      StructField("cell_col", IntegerType, false), StructField("date", StringType, false),
      StructField("future_date", StringType, true), StructField("future_ndvi", DoubleType, true)
    ))
    import scala.collection.JavaConverters._
    val rows = spark.createDataFrame(Seq(
      Row("T1", 0, 0, "2024-01-01", "2024-01-06", 0.3),
      Row("T1", 0, 0, "2024-01-06", null, null) // last observation, no future
    ).asJava, schema2)
    val result = FeatureSplit.withFuturePairingOnly(rows)
    assert(result.count() == 1)
    assert(result.dropDuplicates(Seq("tile","cell_row","cell_col","date")).count() == result.count())
  }
}
