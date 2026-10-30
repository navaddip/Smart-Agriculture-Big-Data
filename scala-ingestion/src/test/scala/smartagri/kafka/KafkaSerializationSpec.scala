package smartagri.kafka

import org.scalatest.funsuite.AnyFunSuite

import smartagri.iot.{IotSchema, ObservableReading}

class KafkaSerializationSpec extends AnyFunSuite {

  private val reading = ObservableReading(
    "T43PHQ-5-12-soil_moisture", "T43PHQ", 5, 12, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.2345)
  )

  test("toJson followed by fromJson reconstructs the original reading exactly") {
    val json = IotSchema.toJson(reading)
    val restored = KafkaSerialization.fromJson(json)
    assert(restored == reading)
  }

  test("serialization and deserialization are both deterministic") {
    assert(IotSchema.toJson(reading) == IotSchema.toJson(reading))
    val json = IotSchema.toJson(reading)
    assert(KafkaSerialization.fromJson(json) == KafkaSerialization.fromJson(json))
  }

  test("a missing value round-trips as None, never Some(0.0) or a parse error") {
    val missingReading = reading.copy(value = None)
    val json = IotSchema.toJson(missingReading)
    val restored = KafkaSerialization.fromJson(json)
    assert(restored.value.isEmpty)
    assert(restored == missingReading)
  }

  test("sensor identity and cell identity survive the round trip exactly") {
    val json = IotSchema.toJson(reading)
    val restored = KafkaSerialization.fromJson(json)
    assert(restored.sensorId == reading.sensorId)
    assert(restored.tile == reading.tile)
    assert(restored.cellRow == reading.cellRow)
    assert(restored.cellCol == reading.cellCol)
  }

  test("eventTime survives the round trip as an exact string match, not just a parseable instant") {
    val json = IotSchema.toJson(reading)
    val restored = KafkaSerialization.fromJson(json)
    assert(restored.eventTime == reading.eventTime)
  }

  test("the serialized payload contains no ground-truth or sensor_health fields") {
    val json = IotSchema.toJson(reading)
    val forbidden = Seq(
      "true_value", "trueValue", "true_soil_moisture", "true_temperature",
      "corruption_type", "corruptionType", "corruption_severity", "corruptionSeverity",
      "sensor_health", "sensorHealth"
    )
    forbidden.foreach(f => assert(!json.contains(f), s"serialized payload must not contain '$f'"))
  }

  test("kafkaKey is deterministic and uses the documented tile:cellRow:cellCol:sensorType format") {
    assert(IotSchema.kafkaKey(reading) == IotSchema.kafkaKey(reading))
    assert(IotSchema.kafkaKey(reading) == "T43PHQ:5:12:soil_moisture")
  }

  test("fromJson throws a clear error on malformed input rather than silently producing a wrong record") {
    assertThrows[IllegalArgumentException](KafkaSerialization.fromJson("not json"))
    assertThrows[IllegalArgumentException](KafkaSerialization.fromJson("""{"sensor_id":"x"}"""))
  }
}
