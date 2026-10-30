package smartagri.kafka

import smartagri.config.AppConfig

/**
 * Centralized Kafka connection/topic settings for the IoT producer and
 * verification consumer (Task 7).
 *
 * Deliberately reuses `AppConfig.default.kafka` (Task 1) rather than
 * re-declaring the bootstrap server or topic independently: `AppConfig`
 * already carries `bootstrapServers = "localhost:9092"` (matching
 * `kafka-compose.yml`'s `KAFKA_ADVERTISED_LISTENERS` exactly) and
 * `iotTopic = "iot_sensor_readings"`. Task 7's own suggested example
 * topic name ("smart-agri-iot") is explicitly NOT used, per Task 7's
 * own instruction to reuse an existing sensible name when one already
 * exists -- it does.
 */
final case class KafkaConfig(
    bootstrapServers: String,
    topic: String,
    clientId: String,
    groupId: String,
    acks: String,
    retries: Int
)

object KafkaConfig {

  /**
   * Default configuration.
   *
   *   - `acks = "all"`: the safe baseline Task 7 asks for -- the
   *     producer waits for the broker (and, in a real multi-replica
   *     cluster, all in-sync replicas) to acknowledge before
   *     considering a send successful. This project's single-broker
   *     KRaft setup has no replication to wait on, so `"all"` behaves
   *     like waiting for the one broker's own acknowledgement here --
   *     still the correct, forward-compatible setting to declare.
   *   - `retries = 3`: a small, non-aggressive retry count -- Task 7
   *     explicitly says not to chase extreme performance/reliability
   *     tuning yet.
   *   - Delivery semantics: AT-LEAST-ONCE, not exactly-once -- exactly
   *     as Task 7 specifies as sufficient for this task. No idempotence
   *     or transactional producer configuration is set up here; a
   *     retried send could in principle be delivered twice, and
   *     duplicate handling is explicitly left to a downstream consumer
   *     (Task 8+), not solved here.
   */
  def default: KafkaConfig = {
    val app = AppConfig.default
    KafkaConfig(
      bootstrapServers = app.kafka.bootstrapServers,
      topic = app.kafka.iotTopic,
      clientId = "smart-agri-iot-producer",
      groupId = "smart-agri-iot-consumer-group",
      acks = "all",
      retries = 3
    )
  }
}
