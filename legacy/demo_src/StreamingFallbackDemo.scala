import org.apache.spark.sql.SparkSession
import smartagri.iot._
import smartagri.integration._
import smartagri.kalman.KalmanConfig

object StreamingFallbackDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StreamingFallbackDemo").master("local[1]")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("Kafka live mode unavailable; local streaming-compatible mode used.")
    println("(Same ObservableReading schema and same StreamingKalmanPipeline.processBatch logic Task 8/9 use for a live Kafka micro-batch -- just fed from a local Task 6-generated batch instead of an actual Kafka poll.)")

    // Real Task 6 simulator, small deterministic cell set
    val cells = Seq(
      IotSimulatorPipeline.CellKey("T43PHQ", 0, 0),
      IotSimulatorPipeline.CellKey("T43PHQ", 0, 1),
      IotSimulatorPipeline.CellKey("T43PGQ", 5, 5)
    ).toDS()
    val config = IotSimulatorConfig.default(seed = 7L).copy(observationsPerCell = 15)
    val (observable, groundTruth) = IotSimulatorPipeline.generate(spark, cells, config)
    observable.cache()
    val obsCount = observable.count()
    println(s"Generated $obsCount observable IoT readings (real Task 6 code, real generation)")

    // Simulate the Task 8 parsed-schema shape (rename to match what parseObservableReadings produces)
    val parsedShape = observable
      .withColumnRenamed("cellRow", "cell_row")
      .withColumnRenamed("cellCol", "cell_col")
      .withColumnRenamed("eventTime", "event_time")
      .withColumnRenamed("sensorType", "sensor_type")

    // Feed through the REAL Task 9 batch processor -- same function a live foreachBatch callback would call
    val kalmanConfig = KalmanConfig.default
    val kalmanOutputs = StreamingKalmanPipeline.processBatch(parsedShape, kalmanConfig)
    println(s"Kalman outputs produced (soil_moisture only, per Task 9's documented scope): ${kalmanOutputs.length}")

    val presentCount = kalmanOutputs.count(_.observedValue.isDefined)
    val missingCount = kalmanOutputs.length - presentCount
    println(s"Present observations processed: $presentCount, missing (predict-only): $missingCount")

    // Path is relative to the project root -- run this from the project root.
    import java.io.{File, PrintWriter}
    new File("results/streaming").mkdirs()
    val pw = new PrintWriter("results/streaming/fallback_demo_summary.csv")
    pw.println("metric,value")
    pw.println(s"mode,local_fallback_no_live_kafka")
    pw.println(s"observable_readings_generated,$obsCount")
    pw.println(s"kalman_outputs_soil_moisture,${kalmanOutputs.length}")
    pw.println(s"present_observations,$presentCount")
    pw.println(s"missing_observations,$missingCount")
    pw.close()
    println("Saved: results/streaming/fallback_demo_summary.csv")

    spark.stop()
    println("STREAMING_FALLBACK_DEMO_COMPLETE")
  }
}
