package smartagri.kafka

import smartagri.iot.ObservableReading

/**
 * Deserializes a Kafka message value back into an `ObservableReading`.
 *
 * The complement to `smartagri.iot.IotSchema.toJson` (Task 6), which
 * this package REUSES for serialization rather than duplicating (Task 7:
 * "do not introduce another serialization format unless necessary" /
 * "reuse the deterministic Task 6 serialization mechanism"). Task 6 had
 * no need for a deserializer (nothing downstream of it consumed JSON);
 * Task 7's consumer does, so that one missing piece is added here, in
 * the Kafka-specific package, without touching `smartagri.iot`.
 *
 * A small, hand-written parser for this ONE small, fixed, flat JSON
 * schema -- not a general-purpose JSON library -- for the same reason
 * Task 6 avoided adding one for serialization: the schema is small,
 * stable, and fully known in advance.
 */
object KafkaSerialization {

  /**
   * Parses the exact JSON shape `IotSchema.toJson` produces:
   * `{"sensor_id":"...","tile":"...","cell_row":N,"cell_col":N,"event_time":"...","sensor_type":"...","value":V}`
   * where `V` is either a bare number or the literal `null`.
   *
   * Throws `IllegalArgumentException` with a clear message on anything
   * that doesn't match this exact shape -- never silently produces a
   * partially-wrong record.
   */
  def fromJson(json: String): ObservableReading = {
    val fields = parseFlatJsonObject(json)

    def required(key: String): String =
      fields.getOrElse(key, throw new IllegalArgumentException(s"missing required field '$key' in: $json"))

    ObservableReading(
      sensorId = required("sensor_id"),
      tile = required("tile"),
      cellRow = required("cell_row").toInt,
      cellCol = required("cell_col").toInt,
      eventTime = required("event_time"),
      sensorType = required("sensor_type"),
      value = required("value") match {
        case "null" => None
        case raw    => Some(raw.toDouble)
      }
    )
  }

  private def parseFlatJsonObject(json: String): Map[String, String] = {
    val trimmed = json.trim
    require(trimmed.startsWith("{") && trimmed.endsWith("}"), s"not a flat JSON object: $json")
    val body = trimmed.substring(1, trimmed.length - 1)
    splitTopLevel(body).map { pair =>
      val idx = pair.indexOf(':')
      require(idx > 0, s"malformed key:value pair '$pair' in: $json")
      val key = unquote(pair.substring(0, idx).trim)
      val rawValue = pair.substring(idx + 1).trim
      val value = if (rawValue.startsWith("\"")) unquote(rawValue) else rawValue
      key -> value
    }.toMap
  }

  private def splitTopLevel(body: String): Seq[String] = {
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder
    var insideString = false
    var i = 0
    while (i < body.length) {
      val c = body.charAt(i)
      if (c == '"' && (i == 0 || body.charAt(i - 1) != '\\')) insideString = !insideString
      if (c == ',' && !insideString) {
        parts += current.toString
        current.clear()
      } else {
        current += c
      }
      i += 1
    }
    if (current.nonEmpty) parts += current.toString
    parts.toSeq
  }

  private def unquote(s: String): String = {
    val t = s.trim
    if (t.startsWith("\"") && t.endsWith("\"") && t.length >= 2)
      t.substring(1, t.length - 1).replace("\\\"", "\"").replace("\\\\", "\\")
    else t
  }
}
