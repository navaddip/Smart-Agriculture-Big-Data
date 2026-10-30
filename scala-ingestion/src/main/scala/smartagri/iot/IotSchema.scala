package smartagri.iot

/**
 * The observable and ground-truth record schemas.
 *
 * DELIBERATELY FLAT (plain `String`/`Int`/`Double`/`Option[Double]`
 * fields, not wrapped in `smartagri.spatial.GridCell` or
 * `smartagri.time.IotEventTime`) -- the same choice Task 5's
 * `KalmanFusionPipeline.NdviObservation` already made, for the same
 * reason: those richer domain types use a private-constructor "smart
 * constructor" pattern (Task 1) that Spark's automatic `Dataset`
 * encoder derivation cannot introspect. Keeping the Spark-facing types
 * flat avoids that friction entirely, at no loss of correctness --
 * `tile`/`cellRow`/`cellCol` are still exactly the project's canonical
 * spatial identity, just not wrapped in the `GridCell` class.
 *
 * WHY TASK 1's EXISTING `smartagri.model.IotSensorReading` IS NOT
 * REUSED HERE (an explicit, documented decision, not an oversight):
 * that type bundles `sensorHealth: SensorHealth` directly into the same
 * case class as the observable measurement fields
 * (`soilMoistureVwc`/`soilTemperatureC`/`airTemperatureC`). Reusing it
 * as the Kafka-observable payload would put `sensor_health` in the
 * observable stream, directly violating Task 6's absolute rule. Per
 * Task 6's explicit instruction to STOP and explain rather than modify
 * an existing file broadly for compatibility, `smartagri.model.IotSensorReading`
 * is left completely untouched, and this package defines its own,
 * cleanly-separated observable/ground-truth pair instead.
 */
final case class ObservableReading(
    sensorId: String,
    tile: String,
    cellRow: Int,
    cellCol: Int,
    eventTime: String,
    sensorType: String,
    value: Option[Double]
)

/** Evaluation-only -- kept in a completely separate dataset (`processed/iot_eval/`), never merged
  * into `ObservableReading`/`processed/iot/`. */
final case class GroundTruthReading(
    sensorId: String,
    tile: String,
    cellRow: Int,
    cellCol: Int,
    eventTime: String,
    sensorType: String,
    trueValue: Double,
    observedValue: Option[Double],
    corruptionType: Option[String],
    corruptionSeverity: Option[Double]
)

object IotSchema {

  /** Deterministic sensor identity from the canonical cell + sensor type -- never a random ID that
    * would change between runs (Task 6's explicit requirement). */
  def sensorId(tile: String, cellRow: Int, cellCol: Int, sensorType: String): String =
    s"$tile-$cellRow-$cellCol-$sensorType"

  /** The Kafka key this reading would use in a future producer (Task 7) -- defined now so the
    * observable schema is already Kafka-shaped, without implementing a producer. */
  def kafkaKey(reading: ObservableReading): String =
    s"${reading.tile}:${reading.cellRow}:${reading.cellCol}:${reading.sensorType}"

  /**
   * Deterministic JSON serialization of one observable reading -- a
   * small, fixed, hand-written serializer rather than a new JSON
   * library dependency, since the schema is small, stable, and fully
   * known here (Task 6: "do not add a heavyweight dependency merely for
   * serialization"). A missing value serializes as a genuine JSON
   * `null` literal, never the string `"null"` or a numeric `0`.
   */
  def toJson(reading: ObservableReading): String = {
    val valueField = reading.value match {
      case Some(v) => v.toString
      case None    => "null"
    }
    s"""{"sensor_id":"${escape(reading.sensorId)}","tile":"${escape(reading.tile)}",""" +
      s""""cell_row":${reading.cellRow},"cell_col":${reading.cellCol},""" +
      s""""event_time":"${escape(reading.eventTime)}","sensor_type":"${escape(reading.sensorType)}",""" +
      s""""value":$valueField}"""
  }

  private def escape(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")
}
