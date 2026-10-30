package smartagri.model

import smartagri.spatial.GridCell
import smartagri.time.{CanonicalDate, IotEventTime}

/**
 * Ground-truth-only label describing whether the (not-yet-implemented)
 * simulator is currently injecting elevated noise/bias for a sensor.
 *
 * This exists purely for evaluating the future adaptive reliability model
 * (reconciliation blueprint §5, Experiments F-J): comparing what the
 * fusion logic *infers* about a sensor's reliability against what the
 * simulator *actually did*.
 *
 * STRICT RULE (Task 1 correction pass, Correction 3): `sensor_health` is
 * SIMULATOR/EVALUATION METADATA. It must NEVER be read by reliability
 * estimation, Kalman fusion, ML, conformal prediction, the decision
 * engine, or streaming state-management logic -- only by an
 * evaluation/experiment step comparing inferred vs. actual reliability.
 * Any of those first five components reading this field is a bug: it
 * would let the system "see the answer" instead of inferring reliability
 * from the observable data, which is the entire point of the adaptive
 * reliability design. See `docs/migration/legacy-compatibility-mapping.md`
 * for the full TRUE STATE -> corruption -> observable-vs-ground-truth
 * split the future simulator must implement.
 */
sealed trait SensorHealth
object SensorHealth {
  case object Ok extends SensorHealth
  case object Degraded extends SensorHealth

  val OkString: String = "OK"
  val DegradedString: String = "DEGRADED"

  def parse(s: String): Either[String, SensorHealth] = s match {
    case OkString       => Right(Ok)
    case DegradedString => Right(Degraded)
    case other          => Left(s"'$other' is not a valid sensor_health value (expected $OkString or $DegradedString)")
  }

  def render(h: SensorHealth): String = h match {
    case Ok       => OkString
    case Degraded => DegradedString
  }
}

/**
 * One simulated IoT sensor reading, per the schema approved in the
 * reconciliation blueprint (§5) and this Phase-3 task (1F).
 *
 * The simulator and Kafka producer that will populate this are NOT
 * implemented in this task -- this is the target schema only, so later
 * tasks (streaming) have a single, already-agreed-upon type to build
 * against instead of inventing the schema again at that point.
 *
 * `airHumidityPct` / `leafWetnessProxy` -- needed only if the optional
 * disease-conducive-risk layer (reconciliation §14 Option B) is approved
 * -- are intentionally NOT included yet, exactly as Task 1F instructs.
 * Adding them now would be speculative for a decision that hasn't been
 * made (reconciliation §27 item 2).
 *
 * `sensorHealth` is evaluation-only ground truth (see `SensorHealth`) and
 * must never be consumed by the fusion/reliability logic once that is
 * built.
 */
final case class IotSensorReading(
    schemaVersion: Int,
    sensorId: String,
    cell: GridCell,
    eventTime: IotEventTime,
    soilMoistureVwc: Double,
    soilTemperatureC: Double,
    airTemperatureC: Double,
    sensorHealth: SensorHealth
) {
  /** The daily analytical date this reading rolls up into. Always derived explicitly -- never implicit (Task 1C). */
  def canonicalDate: CanonicalDate = eventTime.toCanonicalDate

  def canonicalKey: CanonicalKey = CanonicalKey(cell, canonicalDate)
}

object IotSensorReading {
  val CurrentSchemaVersion: Int = 1

  /** Physically valid volumetric water content range (fraction, not percent). */
  val SoilMoistureVwcRange: (Double, Double) = (0.0, 1.0)

  /** Kafka message field / future DataFrame column names, per the approved schema. */
  object Columns {
    val SchemaVersion = "schema_version"
    val SensorId = "sensor_id"
    val EventTime = IotEventTime.ColumnName
    val SoilMoistureVwc = "soil_moisture_vwc"
    val SoilTemperatureC = "soil_temperature_c"
    val AirTemperatureC = "air_temperature_c"
    val SensorHealth = "sensor_health"
    val All: Seq[String] =
      Seq(SchemaVersion, SensorId) ++ GridCell.Columns.All ++
        Seq(EventTime, SoilMoistureVwc, SoilTemperatureC, AirTemperatureC, SensorHealth)
  }
}
