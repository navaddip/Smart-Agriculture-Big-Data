package smartagri.model

import org.scalatest.funsuite.AnyFunSuite
import smartagri.spatial.GridCell
import smartagri.time.IotEventTime

class IotSensorReadingSpec extends AnyFunSuite {

  private val validReading = IotSensorReading(
    schemaVersion = IotSensorReading.CurrentSchemaVersion,
    sensorId = "sensor-001",
    cell = GridCell("T43PHQ", 1, 2),
    eventTime = IotEventTime("2026-09-02T14:03:11Z"),
    soilMoistureVwc = 0.32,
    soilTemperatureC = 24.5,
    airTemperatureC = 29.1,
    sensorHealth = SensorHealth.Ok
  )

  test("a valid IoT reading constructs successfully") {
    assert(validReading.sensorId == "sensor-001")
    assert(validReading.sensorHealth == SensorHealth.Ok)
  }

  test("canonicalDate is explicitly derived from event_time, and canonicalKey combines it with the cell") {
    assert(validReading.canonicalDate.isoString == "2026-09-02")
    assert(validReading.canonicalKey == CanonicalKey(GridCell("T43PHQ", 1, 2), validReading.canonicalDate))
  }

  test("an invalid IoT reading (bad spatial key) throws via the underlying GridCell constructor") {
    assertThrows[IllegalArgumentException](
      validReading.copy(cell = GridCell("", -1, -1))
    )
  }

  test("SensorHealth.parse accepts exactly the two approved values and rejects anything else") {
    assert(SensorHealth.parse("OK") == Right(SensorHealth.Ok))
    assert(SensorHealth.parse("DEGRADED") == Right(SensorHealth.Degraded))
    assert(SensorHealth.parse("ok").isLeft)      // case-sensitive, on purpose -- no silent normalization
    assert(SensorHealth.parse("BROKEN").isLeft)
    assert(SensorHealth.parse(null).isLeft)
  }

  test("SensorHealth.render is the inverse of SensorHealth.parse") {
    assert(SensorHealth.parse(SensorHealth.render(SensorHealth.Ok)) == Right(SensorHealth.Ok))
    assert(SensorHealth.parse(SensorHealth.render(SensorHealth.Degraded)) == Right(SensorHealth.Degraded))
  }

  test("IotSensorReading.Columns.All matches the approved schema field order, spatial key expanded") {
    assert(
      IotSensorReading.Columns.All == Seq(
        "schema_version", "sensor_id", "tile", "cell_row", "cell_col",
        "event_time", "soil_moisture_vwc", "soil_temperature_c", "air_temperature_c", "sensor_health"
      )
    )
  }
}
