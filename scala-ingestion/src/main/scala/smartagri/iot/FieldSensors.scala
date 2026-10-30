package smartagri.iot

/**
 * The in-field IoT sensor types used by the live stream (simulator -> Kafka -> Structured Streaming).
 *
 * Only OBSERVABLE, datasheet-style properties live here (physical bounds, typical measurement
 * noise, how fast the quantity can change). The streaming Kalman filter uses them to set its
 * noise model and the reliability bounds check -- it never sees any simulator ground truth.
 */
final case class FieldSensorSpec(
    name: String,
    unit: String,
    lower: Double,          // physically possible range (reliability bounds check)
    upper: Double,
    typical: Double,        // initial state mean for the filter
    measurementStd: Double, // sensor datasheet noise
    processStdPerHour: Double)

object FieldSensors {
  val SoilMoisture = FieldSensorSpec("soil_moisture", "m3/m3", 0.0, 0.6, 0.25, 0.015, 0.01)
  val SoilTemperature = FieldSensorSpec("soil_temperature", "degC", -5.0, 55.0, 24.0, 0.3, 0.6)
  val AirTemperature = FieldSensorSpec("air_temperature", "degC", -10.0, 55.0, 27.0, 0.4, 1.2)
  val AirHumidity = FieldSensorSpec("air_humidity", "%", 0.0, 100.0, 65.0, 2.0, 4.0)
  val LeafWetness = FieldSensorSpec("leaf_wetness", "fraction", 0.0, 1.0, 0.2, 0.05, 0.15)

  val All: Seq[FieldSensorSpec] = Seq(SoilMoisture, SoilTemperature, AirTemperature, AirHumidity, LeafWetness)
  val ByName: Map[String, FieldSensorSpec] = All.map(s => s.name -> s).toMap
}
