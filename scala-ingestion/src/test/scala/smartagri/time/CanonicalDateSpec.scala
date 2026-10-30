package smartagri.time

import org.scalatest.funsuite.AnyFunSuite

class CanonicalDateSpec extends AnyFunSuite {

  test("a valid ISO-8601 date parses successfully and round-trips its string form") {
    val d = CanonicalDate("2023-04-13")
    assert(d.isoString == "2023-04-13")
    assert(d.year == 2023)
    assert(d.month == 4)
    assert(d.day == 13)
  }

  test("CanonicalDate.parse returns Right for a valid date, without throwing") {
    val result = CanonicalDate.parse("2023-04-13")
    assert(result.isRight)
  }

  test("an invalid/ambiguous date string is rejected, not silently coerced") {
    assertThrows[IllegalArgumentException](CanonicalDate("2023-4-13"))   // unpadded month
    assertThrows[IllegalArgumentException](CanonicalDate("04/13/2023"))  // wrong format entirely
    assertThrows[IllegalArgumentException](CanonicalDate("2023-04-13T00:00:00Z")) // has a time component
    assertThrows[IllegalArgumentException](CanonicalDate("2023-02-30"))  // not a real calendar date
    assertThrows[IllegalArgumentException](CanonicalDate(null))
  }

  test("CanonicalDate.parse returns Left with a message for invalid input, without throwing") {
    val result = CanonicalDate.parse("not-a-date")
    assert(result.isLeft)
    assert(result.left.getOrElse("").nonEmpty)
  }

  test("CanonicalDate.ColumnName is the agreed-upon 'date' column name") {
    assert(CanonicalDate.ColumnName == "date")
  }

  test("a valid IoT event_time parses successfully") {
    val t = IotEventTime("2026-09-02T14:03:11.500Z")
    assert(t.isoString.startsWith("2026-09-02T14:03:11.5"))
  }

  test("an invalid event_time string is rejected") {
    assertThrows[IllegalArgumentException](IotEventTime("2026-09-02")) // date only, not an instant
    assertThrows[IllegalArgumentException](IotEventTime("not-a-timestamp"))
    assertThrows[IllegalArgumentException](IotEventTime(null))
  }

  test("event_time derives its canonical date explicitly, never implicitly") {
    val t = IotEventTime("2026-09-02T23:59:59Z")
    val derived = t.toCanonicalDate
    assert(derived == CanonicalDate("2026-09-02"))
    // The relationship is one-directional: CanonicalDate carries no event_time information.
    assert(derived.isoString == "2026-09-02")
  }

  test("event_time near a UTC day boundary derives the correct UTC calendar date") {
    val justBeforeMidnight = IotEventTime("2026-01-01T23:59:59Z")
    val justAfterMidnight = IotEventTime("2026-01-02T00:00:01Z")
    assert(justBeforeMidnight.toCanonicalDate == CanonicalDate("2026-01-01"))
    assert(justAfterMidnight.toCanonicalDate == CanonicalDate("2026-01-02"))
  }

  // -- Equality / hashing --------------------------------------------------
  // CanonicalDate/IotEventTime are plain classes (private constructor + validating
  // factories), not case classes, so equals/hashCode are hand-written rather than
  // compiler-derived. These tests exist specifically to lock in that behavior
  // (correction pass Task 1, Correction 1).

  test("two CanonicalDates parsed from the same date string are equal and share a hash code") {
    val a = CanonicalDate("2023-04-13")
    val b = CanonicalDate("2023-04-13")
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }

  test("two CanonicalDates for different dates are not equal") {
    assert(CanonicalDate("2023-04-13") != CanonicalDate("2023-04-14"))
  }

  test("a CanonicalDate is never equal to an unrelated type") {
    assert(CanonicalDate("2023-04-13") != "2023-04-13")
    assert(!CanonicalDate("2023-04-13").equals(42))
  }

  test("two IotEventTimes parsed from the same instant are equal and share a hash code") {
    val a = IotEventTime("2026-09-02T14:03:11Z")
    val b = IotEventTime("2026-09-02T14:03:11Z")
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }

  test("two IotEventTimes for different instants are not equal") {
    assert(IotEventTime("2026-09-02T14:03:11Z") != IotEventTime("2026-09-02T14:03:12Z"))
  }
}
