package smartagri.time

import java.time.{Instant, LocalDate, ZoneOffset}
import java.time.format.{DateTimeFormatter, DateTimeParseException}

/**
 * The single canonical *daily analytical* date representation for this
 * project: strict ISO-8601 `YYYY-MM-DD`, no time zone, no time-of-day.
 *
 * This matches what already works today -- the Python-produced
 * `sentinel2_ndvi.parquet` and `era5_weather.parquet` both already use a
 * plain `YYYY-MM-DD` string `date` column. It deliberately does NOT match
 * the existing Scala `Era5Ingestion.scala`, which currently emits an
 * opaque `time_index` integer instead of a real date -- that is a tracked
 * gap, not something this task fixes (see
 * `docs/migration/legacy-compatibility-mapping.md`).
 *
 * `CanonicalDate` is deliberately a distinct type from `IotEventTime`
 * (below). IoT events arrive at sub-daily frequency; the relationship is
 * always
 *
 *     event_time -> (derived) date -> daily state
 *
 * and the derivation must always be explicit (`IotEventTime.toCanonicalDate`
 * / `CanonicalDate.fromEventTime`), never an implicit truncation performed
 * at ingestion time. Collapsing event_time into date at ingestion would
 * throw away exactly the sub-daily staleness signal the adaptive
 * reliability model needs (Phase-1 §4/§6).
 *
 * Construction is via a private constructor plus validating factory
 * methods on the companion object (`apply`/`parse`) -- an ordinary,
 * idiomatic Scala "smart constructor": the only way to obtain a
 * `CanonicalDate` is through those factories, both of which validate
 * first. (An earlier revision used `sealed abstract case class` for
 * this; that was unnecessary complexity left over from working around a
 * case-class-specific quirk that doesn't apply to a plain class, and has
 * been simplified away per the Task 1 correction pass.) `equals`/
 * `hashCode` are overridden by hand below since a plain class doesn't
 * get them for free the way a case class does.
 */
final class CanonicalDate private (val value: LocalDate) {

  /** ISO-8601 string form, e.g. "2023-04-13" -- the only string form this project should store or join on. */
  def isoString: String = value.format(CanonicalDate.Formatter)

  def year: Int = value.getYear
  def month: Int = value.getMonthValue
  def day: Int = value.getDayOfMonth

  override def toString: String = isoString

  override def equals(other: Any): Boolean = other match {
    case that: CanonicalDate => this.value == that.value
    case _ => false
  }

  override def hashCode: Int = value.hashCode
}

object CanonicalDate {
  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE
  private val StrictPattern = """^\d{4}-\d{2}-\d{2}$""".r

  /** The DataFrame/Dataset column name every canonical daily table should use. */
  val ColumnName: String = "date"

  /** Throws on an invalid/ambiguous date string. Prefer `parse` inside a data pipeline. */
  def apply(isoString: String): CanonicalDate =
    parse(isoString).fold(err => throw new IllegalArgumentException(err), identity)

  /**
   * Strict parse: requires exactly `YYYY-MM-DD`. Rejects anything
   * ambiguous or non-conforming -- "2023-4-13", "04/13/2023", a
   * date-with-time string, etc. -- explicitly, rather than having
   * `java.time` silently accept a looser format.
   */
  def parse(isoString: String): Either[String, CanonicalDate] = {
    if (isoString == null) Left("date string is null")
    else if (StrictPattern.findFirstIn(isoString).isEmpty)
      Left(s"'$isoString' is not a strict ISO-8601 date (expected YYYY-MM-DD)")
    else
      try Right(new CanonicalDate(LocalDate.parse(isoString, Formatter)))
      catch { case e: DateTimeParseException => Left(s"'$isoString' is not a valid calendar date: ${e.getMessage}") }
  }

  def fromLocalDate(d: LocalDate): CanonicalDate = new CanonicalDate(d)

  /** Explicit derivation of the daily analytical date from a higher-frequency IoT event timestamp (UTC). */
  def fromEventTime(eventTime: IotEventTime): CanonicalDate =
    new CanonicalDate(eventTime.value.atZone(ZoneOffset.UTC).toLocalDate)
}

/**
 * Raw, higher-frequency IoT observation timestamp: a UTC instant,
 * millisecond precision, ISO-8601 on the wire (e.g.
 * "2026-09-02T14:03:11.500Z").
 *
 * Kept as a distinct type from `CanonicalDate` specifically so an
 * ingestion or streaming job cannot accidentally collapse a raw event
 * timestamp into a daily analytical date without going through the
 * explicit `toCanonicalDate` derivation (Task 1C). Same private-
 * constructor-plus-factory pattern as `CanonicalDate` above, for the
 * same reason.
 */
final class IotEventTime private (val value: Instant) {
  def isoString: String = value.toString
  def toCanonicalDate: CanonicalDate = CanonicalDate.fromEventTime(this)
  def epochMilli: Long = value.toEpochMilli
  override def toString: String = isoString

  override def equals(other: Any): Boolean = other match {
    case that: IotEventTime => this.value == that.value
    case _ => false
  }

  override def hashCode: Int = value.hashCode
}

object IotEventTime {
  /** The DataFrame/Dataset column name the future IoT schema should use for this field. */
  val ColumnName: String = "event_time"

  def apply(isoInstant: String): IotEventTime =
    parse(isoInstant).fold(err => throw new IllegalArgumentException(err), identity)

  def parse(isoInstant: String): Either[String, IotEventTime] =
    if (isoInstant == null) Left("event_time string is null")
    else
      try Right(new IotEventTime(Instant.parse(isoInstant)))
      catch { case e: DateTimeParseException => Left(s"'$isoInstant' is not a valid ISO-8601 UTC instant: ${e.getMessage}") }

  def fromInstant(i: Instant): IotEventTime = new IotEventTime(i)
}
