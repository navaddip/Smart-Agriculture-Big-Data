package smartagri.streaming

import org.apache.spark.sql.types._

/**
 * Spark SQL schemas used to parse the Kafka IoT stream's JSON payload.
 *
 * `ObservableReadingSchema` matches the exact JSON shape
 * `smartagri.iot.IotSchema.toJson` produces (Task 6) -- field NAMES here
 * are the JSON keys (snake_case: `sensor_id`, `cell_row`, ...), not the
 * Scala case class's camelCase field names, since this schema describes
 * the wire format, not the Scala type.
 *
 * `value: DoubleType, nullable = true` is deliberate and load-bearing:
 * a missing observation is meaningful (Task 6's `None`, serialized as a
 * genuine JSON `null` by `IotSchema.toJson`) and must remain `null` all
 * the way through Spark's parsing -- never coerced to `0.0`.
 */
object StreamingSchemas {

  val ObservableReadingSchema: StructType = StructType(
    Seq(
      StructField("sensor_id", StringType, nullable = false),
      StructField("tile", StringType, nullable = false),
      StructField("cell_row", IntegerType, nullable = false),
      StructField("cell_col", IntegerType, nullable = false),
      StructField("event_time", StringType, nullable = false),
      StructField("sensor_type", StringType, nullable = false),
      StructField("value", DoubleType, nullable = true)
    )
  )
}
