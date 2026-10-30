package smartagri.streaming

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import smartagri.iot.{IotSchema, ObservableReading}

class IoTStructuredStreamingSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("IoTStructuredStreamingSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def valueDf(jsonStrings: Seq[String]) = {
    import scala.collection.JavaConverters._
    val schema = StructType(Seq(StructField("value", StringType, nullable = false)))
    spark.createDataFrame(jsonStrings.map(Row(_)).asJava, schema)
  }

  private val reading1 = ObservableReading("T1-0-0-soil_moisture", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.25))
  private val reading2 = ObservableReading("T1-0-1-soil_moisture", "T1", 0, 1, "2024-01-01T00:05:00Z", "soil_moisture", None)

  test("parseObservableReadings correctly parses a valid JSON payload into the expected columns") {
    val df = valueDf(Seq(IotSchema.toJson(reading1)))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    val row = parsed.valid.collect()(0)
    assert(row.getAs[String]("sensor_id") == reading1.sensorId)
    assert(row.getAs[String]("tile") == reading1.tile)
    assert(row.getAs[Int]("cell_row") == reading1.cellRow)
    assert(row.getAs[Int]("cell_col") == reading1.cellCol)
    assert(row.getAs[String]("sensor_type") == reading1.sensorType)
    assert(row.getAs[Double]("value") == reading1.value.get)
    assert(parsed.malformed.count() == 0)
  }

  test("a missing value (JSON null) remains null in the parsed DataFrame, never 0.0") {
    val df = valueDf(Seq(IotSchema.toJson(reading2)))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    val row = parsed.valid.collect()(0)
    assert(row.isNullAt(row.fieldIndex("value")))
  }

  test("event_time is parsed into a real timestamp matching the original instant") {
    val df = valueDf(Seq(IotSchema.toJson(reading1)))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    val row = parsed.valid.collect()(0)
    val ts = row.getAs[java.sql.Timestamp]("event_time")
    assert(ts.toInstant == java.time.Instant.parse(reading1.eventTime))
    assert(row.getAs[String]("event_time_raw") == reading1.eventTime)
  }

  test("the parsed schema contains no ground-truth or sensor_health columns") {
    val df = valueDf(Seq(IotSchema.toJson(reading1)))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    val forbidden = Seq("true_value", "trueValue", "corruption_type", "corruptionType",
      "corruption_severity", "corruptionSeverity", "sensor_health", "sensorHealth")
    forbidden.foreach(f => assert(!parsed.valid.columns.contains(f)))
  }

  test("windowed aggregation computes correct count, average, and missing-value count") {
    val df = valueDf(Seq(IotSchema.toJson(reading1), IotSchema.toJson(reading2)))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    val aggregated = IoTStructuredStreaming.withWatermarkedAggregation(parsed.valid, "10 minutes", "1 hour")
    val row = aggregated.collect()(0)
    assert(row.getAs[Long]("reading_count") == 2L)
    assert(math.abs(row.getAs[Double]("avg_value") - 0.25) < 1e-9)
    assert(row.getAs[Long]("missing_count") == 1L)
  }

  test("malformed JSON is routed to the malformed stream, never treated as a valid reading") {
    val df = valueDf(Seq(IotSchema.toJson(reading1), "not valid json at all", """{"sensor_id":"only one field"}"""))
    val parsed = IoTStructuredStreaming.parseObservableReadings(df)
    assert(parsed.valid.count() == 1)
    assert(parsed.malformed.count() == 2)
  }
}
