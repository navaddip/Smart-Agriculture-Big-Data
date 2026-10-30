package smartagri.mapreduce

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

/**
 * Classification of a single NDVI reading, as determined by the
 * `NdviTileMonthMapper` from the canonical bounds in `SentinelObservation`.
 *
 * Explicit byte representation (documented, per Task 2B's requirement
 * that status be explicit and documented): serialized as a single byte
 * on the wire via `NdviObservationWritable`, using the ordinal values
 * below rather than relying on Java enum ordinal machinery (which is not
 * a stable wire format across JVMs/serialization strategies) -- these
 * three values are written by hand in `write`/`readFields` and must never
 * be reordered without a corresponding format-version bump.
 */
object NdviStatus {
  val Missing: Byte = 0
  val Valid: Byte = 1
  val Invalid: Byte = 2

  def isKnown(b: Byte): Boolean = b == Missing || b == Valid || b == Invalid

  def name(b: Byte): String = b match {
    case Missing => "MISSING"
    case Valid   => "VALID"
    case Invalid => "INVALID"
    case other   => s"UNKNOWN($other)"
  }
}

/**
 * The Mapper-to-Reducer value type for the NDVI tile-month aggregation
 * job: one classified NDVI observation.
 *
 * `status` is one of `NdviStatus.{Missing, Valid, Invalid}`.
 * `value` is the parsed NDVI number for `Valid` and `Invalid` readings
 * (preserved even when invalid, per Task 2B requirement 9, so the
 * Reducer can count -- not discard -- out-of-range readings), and is
 * unused (written/read as `0.0d` but never consulted) for `Missing`
 * readings.
 *
 * Serialization is explicit primitive `DataInput`/`DataOutput` I/O (one
 * byte, one double) -- NOT Java serialization -- exactly as Hadoop's own
 * built-in `Writable` implementations (`Text`, `LongWritable`, etc.) do
 * it, and exactly as Task 2B requires.
 *
 * A no-arg constructor is required by the Hadoop `Writable` contract
 * (Hadoop instantiates value objects via reflection before calling
 * `readFields`); mutability of `status`/`value` after construction is
 * likewise required by that same contract -- Hadoop reuses `Writable`
 * instances across records for performance, so this type intentionally
 * does NOT follow this project's usual immutable-case-class style
 * (Task 1's `GridCell`/`CanonicalDate`, etc.) -- that style is
 * incompatible with how `Writable` is used by the MapReduce framework.
 */
final class NdviObservationWritable(var status: Byte, var value: Double) extends Writable {

  def this() = this(NdviStatus.Missing, 0.0d)

  override def write(out: DataOutput): Unit = {
    out.writeByte(status.toInt)
    out.writeDouble(value)
  }

  /**
   * Reads a status byte and a value double. The status byte is validated
   * IMMEDIATELY here, at the point of deserialization -- a corrupted or
   * unrecognized byte throws `java.io.IOException` (the conventional
   * failure type for a `Writable`'s `readFields`) right away, rather
   * than silently becoming some byte value that only fails later, deep
   * inside `NdviTileMonthAggregator.accumulate`'s catch-all (kept as a
   * second, defense-in-depth check for any instance constructed by
   * means other than deserialization -- but the primary, fail-fast
   * check belongs here, at the actual point where untrusted bytes first
   * become part of this object's state). Per Task 2C's audit
   * requirement: an invalid status value must never silently become one
   * of the three known, valid states.
   */
  override def readFields(in: DataInput): Unit = {
    val readStatus = in.readByte()
    if (!NdviStatus.isKnown(readStatus)) {
      throw new java.io.IOException(
        s"NdviObservationWritable.readFields: unrecognized status byte $readStatus " +
          s"(expected one of Missing=${NdviStatus.Missing}, Valid=${NdviStatus.Valid}, Invalid=${NdviStatus.Invalid})"
      )
    }
    status = readStatus
    value = in.readDouble()
  }

  override def toString: String = s"NdviObservationWritable(${NdviStatus.name(status)}, $value)"
}

object NdviObservationWritable {
  def missing(): NdviObservationWritable = new NdviObservationWritable(NdviStatus.Missing, 0.0d)
  def valid(value: Double): NdviObservationWritable = new NdviObservationWritable(NdviStatus.Valid, value)
  def invalid(value: Double): NdviObservationWritable = new NdviObservationWritable(NdviStatus.Invalid, value)
}
