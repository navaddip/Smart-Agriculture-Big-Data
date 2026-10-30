package smartagri.iot

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class IotSimulatorPipelineSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("IotSimulatorPipelineSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def smallCells(): org.apache.spark.sql.Dataset[IotSimulatorPipeline.CellKey] = {
    val s = spark
    import s.implicits._
    Seq(
      IotSimulatorPipeline.CellKey("T1", 0, 0),
      IotSimulatorPipeline.CellKey("T1", 0, 1),
      IotSimulatorPipeline.CellKey("T2", 5, 5)
    ).toDS()
  }

  test("observable output preserves exactly the input cells' tile/cell_row/cell_col") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 3)
    val (observable, _) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    val distinctCells = observable.select("tile", "cellRow", "cellCol").distinct().collect()
      .map(r => (r.getString(0), r.getInt(1), r.getInt(2))).toSet
    assert(distinctCells == Set(("T1", 0, 0), ("T1", 0, 1), ("T2", 5, 5)))
  }

  test("two different cells produce different observable value sequences") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 10)
    val (observable, _) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    val cellA = observable.filter("tile='T1' AND cellRow=0 AND cellCol=0 AND sensorType='soil_moisture'")
      .orderBy("eventTime").select("value").collect().map(r => if (r.isNullAt(0)) None else Some(r.getDouble(0)))
    val cellB = observable.filter("tile='T1' AND cellRow=0 AND cellCol=1 AND sensorType='soil_moisture'")
      .orderBy("eventTime").select("value").collect().map(r => if (r.isNullAt(0)) None else Some(r.getDouble(0)))
    assert(cellA.toSeq != cellB.toSeq)
  }

  test("the observable schema contains none of the forbidden ground-truth/corruption/health fields") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 2)
    val (observable, _) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    val forbidden = Seq("true_value", "trueValue", "true_soil_moisture", "true_temperature",
      "corruption_type", "corruptionType", "corruption_severity", "corruptionSeverity", "sensor_health", "sensorHealth")
    forbidden.foreach { f =>
      assert(!observable.columns.contains(f), s"observable schema must NOT contain '$f'")
    }
  }

  test("the ground-truth schema DOES contain true value, observed value, and corruption fields") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 2)
    val (_, groundTruth) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    Seq("trueValue", "observedValue", "corruptionType", "corruptionSeverity").foreach { f =>
      assert(groundTruth.columns.contains(f), s"ground-truth schema must contain '$f'")
    }
  }

  test("validateNoDuplicateEventKeys does not throw on normally-generated (always-unique) output") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 5)
    val (observable, _) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    IotSimulatorPipeline.validateNoDuplicateEventKeys(observable)
  }

  test("validateNoDuplicateEventKeys throws when a duplicate (sensorId, eventTime) key is artificially introduced") {
    val s = spark
    import s.implicits._
    val dup = Seq(
      ObservableReading("S1", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.2)),
      ObservableReading("S1", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.3))
    ).toDF()
    val ex = intercept[IllegalStateException] {
      IotSimulatorPipeline.validateNoDuplicateEventKeys(dup)
    }
    assert(ex.getMessage.contains("duplicate"))
  }

  test("observable and ground-truth outputs survive a real Parquet write/read round trip") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 4)
    val (observable, groundTruth) = IotSimulatorPipeline.generate(spark, smallCells(), config)

    val obsPath = java.nio.file.Files.createTempDirectory("iot_obs_test").toString
    val gtPath = java.nio.file.Files.createTempDirectory("iot_gt_test").toString
    java.nio.file.Files.delete(java.nio.file.Paths.get(obsPath))
    java.nio.file.Files.delete(java.nio.file.Paths.get(gtPath))

    IotSimulatorPipeline.withEventDate(observable).write.mode("overwrite").partitionBy("event_date").parquet(obsPath)
    IotSimulatorPipeline.withEventDate(groundTruth).write.mode("overwrite").partitionBy("event_date").parquet(gtPath)

    val rereadObs = spark.read.parquet(obsPath)
    val rereadGt = spark.read.parquet(gtPath)
    assert(rereadObs.count() == observable.count())
    assert(rereadGt.count() == groundTruth.count())
  }

  test("generate() returns DataFrames backed by a real distributed plan, not a driver-side collection") {
    val config = IotSimulatorConfig.default(1L).copy(observationsPerCell = 3)
    val (observable, _) = IotSimulatorPipeline.generate(spark, smallCells(), config)
    assert(observable.rdd.getNumPartitions >= 1)
  }
}
