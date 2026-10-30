package smartagri.mapreduce

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests the real Hadoop `Writable` contract (`write`/`readFields` via
 * `DataOutput`/`DataInput`) directly, without any Hadoop cluster,
 * `Job`, or `Context` -- exactly the kind of "test Hadoop classes
 * directly in a clean local unit test" Task 2B asks for where possible.
 * Uses plain `java.io.DataOutputStream`/`DataInputStream` over an
 * in-memory byte array, which is a real, general-purpose
 * `DataOutput`/`DataInput` implementation, not a Hadoop-specific stub.
 */
class NdviObservationWritableSpec extends AnyFunSuite {

  private def roundTrip(original: NdviObservationWritable): NdviObservationWritable = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    original.write(out)
    out.flush()

    val in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val restored = new NdviObservationWritable() // exercises the required no-arg constructor
    restored.readFields(in)
    restored
  }

  test("a VALID observation survives a write/readFields round trip exactly") {
    val original = NdviObservationWritable.valid(0.42)
    val restored = roundTrip(original)
    assert(restored.status == NdviStatus.Valid)
    assert(restored.value == 0.42)
  }

  test("a MISSING observation survives a write/readFields round trip exactly") {
    val original = NdviObservationWritable.missing()
    val restored = roundTrip(original)
    assert(restored.status == NdviStatus.Missing)
  }

  test("an INVALID observation survives a write/readFields round trip, including an out-of-range value") {
    val original = NdviObservationWritable.invalid(999.0)
    val restored = roundTrip(original)
    assert(restored.status == NdviStatus.Invalid)
    assert(restored.value == 999.0)
  }

  test("write/readFields uses explicit primitive I/O, not Java serialization (no Serializable marker needed)") {
    // If this compiles and the round trip above passes without NdviObservationWritable extending
    // java.io.Serializable, that IS the evidence -- asserted explicitly here as a regression guard.
    assert(!classOf[NdviObservationWritable].getInterfaces.exists(_ == classOf[java.io.Serializable]))
    assert(classOf[NdviObservationWritable].getInterfaces.exists(_ == classOf[org.apache.hadoop.io.Writable]))
  }

  test("the serialized form is a fixed, small size: exactly one byte (status) plus one double (8 bytes)") {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    NdviObservationWritable.valid(0.42).write(out)
    out.flush()
    assert(bytes.toByteArray.length == 9) // 1 (byte) + 8 (double)
  }

  test("readFields throws IOException immediately when the status byte is unrecognized (Task 2C hardening)") {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    out.writeByte(5) // not Missing(0), Valid(1), or Invalid(2)
    out.writeDouble(0.0)
    out.flush()

    val in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray))
    val target = new NdviObservationWritable()
    val ex = intercept[java.io.IOException] {
      target.readFields(in)
    }
    assert(ex.getMessage.contains("5"))
    // The object must not have silently adopted the bad byte as its status:
    assert(target.status == NdviStatus.Missing) // unchanged from the no-arg constructor's default
  }

  test("NdviStatus.isKnown correctly distinguishes the three valid statuses from an arbitrary byte") {
    assert(NdviStatus.isKnown(NdviStatus.Missing))
    assert(NdviStatus.isKnown(NdviStatus.Valid))
    assert(NdviStatus.isKnown(NdviStatus.Invalid))
    assert(!NdviStatus.isKnown(5.toByte))
    assert(!NdviStatus.isKnown((-1).toByte))
  }

  test("the no-arg constructor required by the Writable contract defaults to MISSING/0.0") {
    val fresh = new NdviObservationWritable()
    assert(fresh.status == NdviStatus.Missing)
    assert(fresh.value == 0.0)
  }

  test("NdviStatus.name renders all three known statuses distinctly, for debugging/toString use") {
    assert(NdviStatus.name(NdviStatus.Missing) == "MISSING")
    assert(NdviStatus.name(NdviStatus.Valid) == "VALID")
    assert(NdviStatus.name(NdviStatus.Invalid) == "INVALID")
  }
}
