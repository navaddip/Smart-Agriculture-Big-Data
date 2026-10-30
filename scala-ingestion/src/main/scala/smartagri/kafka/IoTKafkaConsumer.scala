package smartagri.kafka

import java.time.Duration
import java.util.{Collections, Properties}

import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import smartagri.iot.ObservableReading

/**
 * A small, BOUNDED verification consumer -- NOT a permanent streaming
 * service. Task 8 builds the real, ongoing consumption path with Spark
 * Structured Streaming; this class exists only to prove that records
 * published by `IoTKafkaProducer` can actually be read back correctly.
 *
 * Same sandbox-compilation caveat as `IoTKafkaProducer` -- see that
 * class's doc comment for the full explanation. This file could not be
 * compiled or executed in this session for the same reason (no
 * `kafka-clients` jar obtainable here).
 */
class IoTKafkaConsumer(config: KafkaConfig) extends AutoCloseable {

  private val consumer: KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    // "earliest" so a verification run started after the producer has already sent its records
    // (the normal case for a short-lived test/verification run) still reads them, rather than
    // silently seeing nothing because it joined "too late" under the default "latest" offset reset.
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    new KafkaConsumer[String, String](props)
  }

  consumer.subscribe(Collections.singletonList(config.topic))

  /**
   * Polls until `maxRecords` have been read or `overallTimeoutMillis`
   * elapses, whichever comes first -- a BOUNDED verification read, never
   * an unbounded streaming loop (that is Task 8's job). Deserializes
   * every record via `KafkaSerialization.fromJson`, so a malformed
   * message would surface as a thrown exception here rather than being
   * silently skipped.
   */
  def readUpTo(maxRecords: Int, pollTimeout: Duration, overallTimeoutMillis: Long): Seq[ObservableReading] = {
    val results = scala.collection.mutable.ArrayBuffer.empty[ObservableReading]
    val deadline = System.currentTimeMillis() + overallTimeoutMillis
    while (results.length < maxRecords && System.currentTimeMillis() < deadline) {
      val records = consumer.poll(pollTimeout)
      records.asScala.foreach { r =>
        results += KafkaSerialization.fromJson(r.value())
      }
    }
    results.toSeq
  }

  override def close(): Unit = consumer.close()
}
