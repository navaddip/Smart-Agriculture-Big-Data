package smartagri.kafka

import org.scalatest.funsuite.AnyFunSuite

class KafkaConfigSpec extends AnyFunSuite {

  test("default config reuses AppConfig's bootstrap servers, matching kafka-compose.yml") {
    assert(KafkaConfig.default.bootstrapServers == "localhost:9092")
  }

  test("default config reuses AppConfig's existing IoT topic name, not a newly-invented one") {
    assert(KafkaConfig.default.topic == "iot_sensor_readings")
  }

  test("default config uses acks=all, the safe baseline Task 7 requires") {
    assert(KafkaConfig.default.acks == "all")
  }

  test("default config uses a small, non-aggressive retry count") {
    assert(KafkaConfig.default.retries > 0 && KafkaConfig.default.retries <= 5)
  }

  test("clientId and groupId are non-empty and distinct from each other") {
    val cfg = KafkaConfig.default
    assert(cfg.clientId.nonEmpty)
    assert(cfg.groupId.nonEmpty)
    assert(cfg.clientId != cfg.groupId)
  }
}
