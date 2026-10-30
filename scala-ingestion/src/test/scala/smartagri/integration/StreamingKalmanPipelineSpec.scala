package smartagri.integration

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import smartagri.kalman.KalmanConfig

class StreamingKalmanPipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("StreamingKalmanPipelineSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val batchSchema = StructType(Seq(
    StructField("tile", StringType, false),
    StructField("cell_row", IntegerType, false),
    StructField("cell_col", IntegerType, false),
    StructField("event_time", StringType, false),
    StructField("sensor_type", StringType, false),
    StructField("value", DoubleType, true)
  ))

  private def batchDf(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, batchSchema)
  }

  private val config = KalmanConfig.default

  test("only soil_moisture readings are processed; air_temperature/soil_temperature readings for the same cell are excluded, not merged in") {
    val batch = batchDf(Seq(
      Row("T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", 0.25),
      Row("T1", 0, 0, "2024-01-01T00:00:00Z", "air_temperature", 26.0),
      Row("T1", 0, 0, "2024-01-01T00:00:00Z", "soil_temperature", 22.0)
    ))
    val outputs = StreamingKalmanPipeline.processBatch(batch, config)
    assert(outputs.length == 1, "only the soil_moisture reading should produce a Kalman output")
    assert(outputs.head.sensorType == "soil_moisture")
    assert(outputs.head.observedValue.contains(0.25))
  }

  test("two different cells' soil_moisture readings are processed as two independent Kalman tracks, not merged") {
    val batch = batchDf(Seq(
      Row("T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", 0.10),
      Row("T1", 0, 1, "2024-01-01T00:00:00Z", "soil_moisture", 0.90)
    ))
    val outputs = StreamingKalmanPipeline.processBatch(batch, config)
    assert(outputs.length == 2)
    val cellA = outputs.find(o => o.cellRow == 0 && o.cellCol == 0).get
    val cellB = outputs.find(o => o.cellRow == 0 && o.cellCol == 1).get
    assert(math.abs(cellA.kalmanState - cellB.kalmanState) > 0.1)
  }

  test("T1's state does not depend on T2/T3, and observations are sorted by event_time before processing, regardless of batch row order") {
    val batch = batchDf(Seq(
      Row("T1", 0, 0, "2024-01-10T00:00:00Z", "soil_moisture", 0.90),
      Row("T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", 0.20),
      Row("T1", 0, 0, "2024-01-05T00:00:00Z", "soil_moisture", 0.20)
    ))
    val outputs = StreamingKalmanPipeline.processBatch(batch, config).sortBy(_.eventTime)
    assert(outputs.map(_.eventTime) == Seq("2024-01-01T00:00:00Z", "2024-01-05T00:00:00Z", "2024-01-10T00:00:00Z"),
      "processing order must be chronological regardless of input row order")

    val t1OnlyBatch = batchDf(Seq(Row("T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", 0.20)))
    val t1OnlyOutput = StreamingKalmanPipeline.processBatch(t1OnlyBatch, config).head
    val t1WithFutureOutput = outputs.head
    assert(t1WithFutureOutput.eventTime == "2024-01-01T00:00:00Z")
    assert(math.abs(t1OnlyOutput.kalmanState - t1WithFutureOutput.kalmanState) < 1e-12,
      "T1's state must be identical whether or not T2/T3 exist later in the same batch")
  }

  test("processBatch operates correctly on a small, explicitly bounded batch (simulating a real micro-batch)") {
    val batch = batchDf((1 to 5).map(i => Row("T1", 0, 0, f"2024-01-0$i%dT00:00:00Z", "soil_moisture", 0.2 + i * 0.01)))
    val outputs = StreamingKalmanPipeline.processBatch(batch, config)
    assert(outputs.length == 5)
  }
}
