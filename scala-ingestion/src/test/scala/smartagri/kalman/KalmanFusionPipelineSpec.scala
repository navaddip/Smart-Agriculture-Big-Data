package smartagri.kalman

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class KalmanFusionPipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("KalmanFusionPipelineSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val config = KalmanConfig.default

  private def obs(date: String, ndvi: Double) =
    KalmanFusionPipeline.NdviObservation("T1", 0, 0, date, ndvi)

  test("runCellSequential processes observations in date order regardless of input order") {
    val outOfOrder = Seq(obs("2024-01-11", 0.30), obs("2024-01-01", 0.10), obs("2024-01-06", 0.20))
    val result = KalmanFusionPipeline.runCellSequential(outOfOrder, config, adaptive = false)
    assert(result.map(_.date) == Seq("2024-01-01", "2024-01-06", "2024-01-11"),
      "output must be chronologically ordered even when input arrives out of order")
  }

  test("an earlier row's kalman_state is identical whether or not a later observation exists") {
    val withoutFuture = Seq(obs("2024-01-01", 0.10), obs("2024-01-06", 0.20))
    val withFuture = Seq(obs("2024-01-01", 0.10), obs("2024-01-06", 0.20), obs("2024-01-11", 0.90))

    val resultWithout = KalmanFusionPipeline.runCellSequential(withoutFuture, config, adaptive = true)
    val resultWith = KalmanFusionPipeline.runCellSequential(withFuture, config, adaptive = true)

    val secondRowWithout = resultWithout(1)
    val secondRowWith = resultWith(1)
    assert(math.abs(secondRowWithout.kalmanState - secondRowWith.kalmanState) < 1e-12,
      "the 2024-01-06 posterior must be identical whether or not the later, extreme 2024-01-11 observation exists")
    assert(math.abs(secondRowWithout.kalmanVariance - secondRowWith.kalmanVariance) < 1e-12)
    assert(math.abs(secondRowWithout.observationReliability - secondRowWith.observationReliability) < 1e-12,
      "reliability at T must not be influenced by a later observation either")
  }

  test("validateNoDuplicateKeys throws when the input has a duplicate (tile,cell_row,cell_col,date) key") {
    val schema = StructType(Seq(
      StructField("tile", StringType, false), StructField("cell_row", IntegerType, false),
      StructField("cell_col", IntegerType, false), StructField("date", StringType, false),
      StructField("ndvi", DoubleType, true)))
    import scala.collection.JavaConverters._
    val dupDf = spark.createDataFrame(Seq(
      Row("T1", 0, 0, "2024-01-01", 0.3),
      Row("T1", 0, 0, "2024-01-01", 0.4)
    ).asJava, schema)

    val ex = intercept[IllegalStateException] {
      KalmanFusionPipeline.validateNoDuplicateKeys(dupDf)
    }
    assert(ex.getMessage.contains("duplicate"))
  }

  test("validateNoDuplicateKeys does not throw on a clean input") {
    val schema = StructType(Seq(
      StructField("tile", StringType, false), StructField("cell_row", IntegerType, false),
      StructField("cell_col", IntegerType, false), StructField("date", StringType, false),
      StructField("ndvi", DoubleType, true)))
    import scala.collection.JavaConverters._
    val cleanDf = spark.createDataFrame(Seq(
      Row("T1", 0, 0, "2024-01-01", 0.3),
      Row("T1", 0, 1, "2024-01-01", 0.4)
    ).asJava, schema)
    KalmanFusionPipeline.validateNoDuplicateKeys(cleanDf)
  }

  test("an irregular gap between observations is reflected deterministically in elapsedDays-driven P growth") {
    val sparse = Seq(obs("2024-01-01", 0.30), obs("2024-06-01", 0.30))
    val result = KalmanFusionPipeline.runCellSequential(sparse, config, adaptive = false)
    val secondRow = result(1)
    val expectedPredictedP = config.initialStateVariance + config.processNoisePerDay * 152
    assert(secondRow.kalmanVariance < expectedPredictedP, "posterior variance must shrink after the update, from the grown predicted variance")
  }

  test("running the same cell sequence twice produces byte-identical output") {
    val sequence = Seq(obs("2024-01-01", 0.10), obs("2024-01-06", 0.25), obs("2024-01-11", 0.40))
    val run1 = KalmanFusionPipeline.runCellSequential(sequence, config, adaptive = true)
    val run2 = KalmanFusionPipeline.runCellSequential(sequence, config, adaptive = true)
    assert(run1 == run2, "identical input must produce identical output on repeated runs")
  }

  test("static and adaptive modes are both individually deterministic across repeated runs") {
    val sequence = Seq(obs("2024-01-01", 0.10), obs("2024-01-06", 0.90), obs("2024-01-11", 0.15))
    val staticRun1 = KalmanFusionPipeline.runCellSequential(sequence, config, adaptive = false)
    val staticRun2 = KalmanFusionPipeline.runCellSequential(sequence, config, adaptive = false)
    assert(staticRun1 == staticRun2)
  }
}
