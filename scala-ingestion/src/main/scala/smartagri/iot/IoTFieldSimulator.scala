package smartagri.iot

import java.time.Instant

import scala.util.Random

import smartagri.kafka.{IoTKafkaProducer, KafkaConfig}
import smartagri.preprocess.PreprocessSupport

/**
 * Stand-in for real farm IoT devices (none are deployed yet): publishes live sensor readings to Kafka.
 *
 *   java -cp app.jar smartagri.iot.IoTFieldSimulator --bootstrap kafka:29092 --cells 200 --seconds 180
 *
 * Every tick (1 s) each simulated field (a real Sentinel grid cell) emits one reading per sensor
 * type, as ObservableReading JSON, keyed by tile:row:col:sensor. Fields are assigned a condition
 * that the downstream stream processor has to discover on its own:
 *   ~15% humid microclimate (fungal-disease-favourable), ~15% drying soil (water stress), rest normal.
 * Real-device faults are injected: 3% spikes (outliers), 3% dropped readings (value = null).
 *
 * Only the observed value is ever published -- the true value never leaves this process.
 */
object IoTFieldSimulator {

  sealed trait Condition { def name: String }
  case object Normal extends Condition { val name = "normal" }
  case object HumidDisease extends Condition { val name = "humid_microclimate" }
  case object Drying extends Condition { val name = "drying_soil" }

  /** Long-run mean of each sensor under each field condition. */
  def targetMean(sensor: FieldSensorSpec, cond: Condition): Double = (sensor.name, cond) match {
    case ("air_humidity", HumidDisease) => 94.0
    case ("leaf_wetness", HumidDisease) => 0.8
    case ("air_temperature", HumidDisease) => 23.0
    case ("soil_moisture", Drying) => 0.07
    case ("soil_temperature", Drying) => 31.0
    case ("air_humidity", Drying) => 38.0
    case ("leaf_wetness", Drying) => 0.02
    case _ => sensor.typical
  }

  final class SensorProcess(val spec: FieldSensorSpec, val mean: Double, seed: Long) {
    private val rng = new Random(seed)
    private var x = mean + rng.nextGaussian() * spec.processStdPerHour
    /** AR(1) step, then measurement noise and device faults. Returns the OBSERVED value only. */
    def next(outlierP: Double, dropoutP: Double): Option[Double] = {
      x = mean + 0.9 * (x - mean) + rng.nextGaussian() * spec.processStdPerHour * 0.3
      x = math.max(spec.lower, math.min(spec.upper, x))
      val observed = x + rng.nextGaussian() * spec.measurementStd
      if (rng.nextDouble() < dropoutP) None
      else if (rng.nextDouble() < outlierP) Some(observed + (if (rng.nextBoolean()) 1 else -1) * (spec.upper - spec.lower) * 0.6)
      else Some(math.round(observed * 1000) / 1000.0)
    }
  }

  def main(args: Array[String]): Unit = {
    val opts = PreprocessSupport.parseArgs(args)
    val bootstrap = opts.getOrElse("bootstrap", "kafka:29092")
    val topic = opts.getOrElse("topic", "iot_sensor_readings")
    val nCells = opts.getOrElse("cells", "200").toInt
    val seconds = opts.getOrElse("seconds", "180").toInt
    val outlierP = opts.getOrElse("outlier-rate", "0.03").toDouble
    val dropoutP = opts.getOrElse("dropout-rate", "0.03").toDouble

    val rng = new Random(7)
    val cells = (0 until nCells).map { i =>
      val tile = if (i % 2 == 0) "T43PGQ" else "T43PHQ"
      val r = rng.nextInt(100); val c = rng.nextInt(100)
      val u = rng.nextDouble()
      val cond = if (u < 0.15) HumidDisease else if (u < 0.30) Drying else Normal
      (tile, r, c, cond)
    }.distinct
    val processes = cells.flatMap { case (tile, r, c, cond) =>
      FieldSensors.All.map { s =>
        val sid = IotSchema.sensorId(tile, r, c, s.name)
        (tile, r, c, s, new SensorProcess(s, targetMean(s, cond), sid.hashCode.toLong))
      }
    }
    // Hidden field conditions, written ONLY for offline scoring of the stream's alerts
    // (smartagri.iot.StreamingDetectionEvaluation). Never published to Kafka.
    val truthFile = new java.io.File(opts.getOrElse("truth-out", "/results/streaming/field_conditions_truth.csv"))
    truthFile.getParentFile.mkdirs()
    val pw = new java.io.PrintWriter(truthFile, "UTF-8")
    try { pw.println("tile,cell_row,cell_col,condition"); cells.foreach { case (t, r, c, cond) => pw.println(s"$t,$r,$c,${cond.name}") } }
    finally pw.close()

    println(s"Simulating ${cells.size} fields x ${FieldSensors.All.size} sensors -> Kafka $bootstrap topic '$topic' for $seconds s")
    cells.groupBy(_._4.name).foreach { case (k, v) => println(s"  $k: ${v.size} fields") }

    val producer = new IoTKafkaProducer(KafkaConfig.default.copy(bootstrapServers = bootstrap, topic = topic))
    var sent = 0L
    try {
      val end = System.currentTimeMillis() + seconds * 1000L
      while (System.currentTimeMillis() < end) {
        val tickStart = System.currentTimeMillis()
        val now = Instant.ofEpochMilli(tickStart).toString
        processes.foreach { case (tile, r, c, s, p) =>
          producer.send(ObservableReading(IotSchema.sensorId(tile, r, c, s.name), tile, r, c, now, s.name, p.next(outlierP, dropoutP)))
          sent += 1
        }
        if ((tickStart / 1000) % 15 == 0) println(s"  $now sent=$sent")
        val sleep = 1000 - (System.currentTimeMillis() - tickStart)
        if (sleep > 0) Thread.sleep(sleep)
      }
    } finally producer.close()
    println(s"Done: $sent readings published to '$topic'")
  }
}
