package smartagri.iot

import org.scalatest.funsuite.AnyFunSuite

class EventTimeAndSchemaSpec extends AnyFunSuite {

  test("generated event timestamps are all valid, parseable ISO-8601 instants") {
    val times = EventTimeGenerator.generate("2024-01-01T00:00:00Z", 3600, 0, 10, 1L)
    times.foreach(t => java.time.Instant.parse(t))
    assert(times.length == 10)
  }

  test("regular sampling (jitter=0) produces exactly intervalSeconds between consecutive timestamps") {
    val times = EventTimeGenerator.generate("2024-01-01T00:00:00Z", 3600, 0, 5, 1L).map(java.time.Instant.parse)
    times.sliding(2).foreach { case Seq(a, b) => assert(java.time.Duration.between(a, b).getSeconds == 3600) }
  }

  test("irregular sampling (jitter>0) produces non-uniform, but always positive, intervals") {
    val times = EventTimeGenerator.generate("2024-01-01T00:00:00Z", 3600, 600, 20, 1L).map(java.time.Instant.parse)
    val gaps = times.sliding(2).map { case Seq(a, b) => java.time.Duration.between(a, b).getSeconds }.toSeq
    assert(gaps.distinct.length > 1, "irregular sampling should not produce identical gaps every time")
    assert(gaps.forall(_ > 0), "every gap must remain strictly positive (no collisions/reordering)")
    assert(gaps.forall(g => math.abs(g - 3600) <= 600), "every gap must stay within the configured jitter bound")
  }

  test("event time generation is deterministic under a fixed seed") {
    val t1 = EventTimeGenerator.generate("2024-01-01T00:00:00Z", 3600, 600, 10, 99L)
    val t2 = EventTimeGenerator.generate("2024-01-01T00:00:00Z", 3600, 600, 10, 99L)
    assert(t1 == t2)
  }

  test("sensorId is a deterministic function of cell identity and sensor type") {
    val id1 = IotSchema.sensorId("T43PHQ", 5, 12, "soil_moisture")
    val id2 = IotSchema.sensorId("T43PHQ", 5, 12, "soil_moisture")
    assert(id1 == id2)
    assert(id1 == "T43PHQ-5-12-soil_moisture")
  }

  test("sensorId differs for different cells or sensor types") {
    val a = IotSchema.sensorId("T43PHQ", 5, 12, "soil_moisture")
    val b = IotSchema.sensorId("T43PHQ", 5, 13, "soil_moisture")
    val c = IotSchema.sensorId("T43PHQ", 5, 12, "air_temperature")
    assert(a != b)
    assert(a != c)
  }

  test("toJson is deterministic and correctly represents a present value") {
    val reading = ObservableReading("T1-0-0-soil_moisture", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.234))
    val json1 = IotSchema.toJson(reading)
    val json2 = IotSchema.toJson(reading)
    assert(json1 == json2)
    assert(json1.contains("\"value\":0.234"))
    assert(json1.contains("\"sensor_id\":\"T1-0-0-soil_moisture\""))
  }

  test("toJson represents a missing value as a genuine JSON null, never 0 or a string") {
    val reading = ObservableReading("T1-0-0-soil_moisture", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", None)
    val json = IotSchema.toJson(reading)
    assert(json.contains("\"value\":null"))
    assert(!json.contains("\"value\":0"))
    assert(!json.contains("\"value\":\"null\""))
  }

  test("kafkaKey is a deterministic function of the reading's cell and sensor type") {
    val reading = ObservableReading("T1-0-0-soil_moisture", "T1", 0, 0, "2024-01-01T00:00:00Z", "soil_moisture", Some(0.2))
    assert(IotSchema.kafkaKey(reading) == "T1:0:0:soil_moisture")
  }
}
