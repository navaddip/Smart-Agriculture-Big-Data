package smartagri.mapreduce

import scala.util.Try

import smartagri.model.SentinelObservation
import smartagri.time.CanonicalDate

/**
 * Pure, Hadoop-independent parsing/classification logic for one line of
 * the Task 2A flat NDVI export.
 *
 * Isolated here specifically so it can be unit-tested directly, without
 * a Hadoop `Mapper.Context` or a live MapReduce run (Task 2B's testing
 * requirement) -- `NdviTileMonthMapper` is a thin wrapper that calls
 * straight into this object and does nothing else parsing-related
 * itself.
 */
object NdviRecordParser {

  /** A successfully parsed line: its grouping-key components and its classified observation. */
  final case class ParsedRecord(tile: String, yearMonth: String, observation: NdviObservationWritable)

  sealed trait ParseResult
  final case class Parsed(record: ParsedRecord) extends ParseResult
  final case class Malformed(reason: String) extends ParseResult

  /**
   * Must match Task 2A's `NdviFlatInputPreparation.ExportColumns.length`.
   * Duplicated as a literal here (rather than importing Task 2A's object)
   * because this module is deliberately Spark-free -- it must be usable
   * from a plain Hadoop `Mapper` with no Spark on the classpath at
   * runtime. `NdviRecordParserSpec` asserts this literal stays equal to
   * Task 2A's value, so the two can't silently drift apart.
   */
  val ExpectedFieldCount: Int = 5

  /**
   * Must match Task 2A's `NdviFlatInputPreparation.MissingNdviToken`
   * exactly -- this is the output contract Task 2A documented as being
   * handed to this Mapper. Duplicated as a literal for the same
   * Spark-independence reason as `ExpectedFieldCount`, and covered by
   * the same cross-check test.
   */
  val MissingNdviToken: String = "NA"

  /** Reused, never retyped, per Task 2B's explicit instruction. */
  private val NdviMin: Double = SentinelObservation.NdviMin
  private val NdviMax: Double = SentinelObservation.NdviMax

  /**
   * Derives `YYYY-MM` from an already-validated `CanonicalDate`. Safe by
   * construction: `CanonicalDate` (Task 1) only ever holds a strictly
   * validated `YYYY-MM-DD` string, so taking the first 7 characters is
   * never ambiguous.
   */
  def yearMonthOf(date: CanonicalDate): String = date.isoString.substring(0, 7)

  /** The reserved separator character used by `groupingKey`. Never valid inside a `tile` value (see
    * `parseLine`'s explicit guard against it), and never appears in `yearMonth` (always `YYYY-MM`). */
  val GroupingKeySeparator: Char = '|'

  /** The canonical Hadoop grouping-key text for a (tile, year_month) pair, e.g. `"T43PHQ|2023-04"`. */
  def groupingKey(tile: String, yearMonth: String): String = s"$tile$GroupingKeySeparator$yearMonth"

  /**
   * Parses one tab-delimited line into a `ParsedRecord`, or reports why
   * it's malformed. Never throws -- a bad line is data for the Mapper to
   * react to (count and skip), not an exception that should propagate
   * out of a Mapper and fail an entire distributed job over one bad
   * record (Task 2B requirement 2).
   */
  def parseLine(line: String): ParseResult = {
    if (line == null) {
      Malformed("line is null")
    } else {
      val fields = line.split("\t", -1)
      if (fields.length != ExpectedFieldCount) {
        Malformed(s"expected $ExpectedFieldCount tab-separated fields, found ${fields.length}: '$line'")
      } else {
        val tile = fields(0)
        val dateRaw = fields(1)
        val cellRowRaw = fields(2)
        val cellColRaw = fields(3)
        val ndviRaw = fields(4)

        if (tile.isEmpty) {
          Malformed(s"empty tile field: '$line'")
        } else if (tile.contains(GroupingKeySeparator)) {
          // A tile containing the grouping-key separator would make groupingKey's "tile|year_month"
          // format ambiguous to split back apart in the Reducer (e.g. tile "A|B" would produce key
          // "A|B|2023-04", which no longer unambiguously separates into tile="A|B" / year_month=
          // "2023-04" without extra care). Real Sentinel-2 tile IDs (MGRS designators, e.g.
          // "T43PHQ") never contain '|', so this never fires against real data -- it exists as an
          // explicit guard against a corrupted or unexpected tile field silently producing an
          // ambiguous key, per Task 2C's audit finding, rather than trusting the assumption
          // implicitly.
          Malformed(s"tile field contains the reserved grouping-key separator '$GroupingKeySeparator': '$line'")
        } else {
          CanonicalDate.parse(dateRaw) match {
            case Left(dateError) =>
              Malformed(s"invalid date '$dateRaw': $dateError")
            case Right(date) =>
              if (Try(cellRowRaw.toInt).isFailure || Try(cellColRaw.toInt).isFailure) {
                // cell_row/cell_col are not part of this job's grouping key (tile-month aggregation
                // is intentional, per Task 2B), but a record whose spatial fields aren't even
                // parseable as integers is evidence of a genuinely corrupt line, not a fine record
                // we simply ignore two fields of -- rejected outright, consistent with "reject
                // malformed records explicitly rather than silently producing corrupt statistics."
                Malformed(s"non-numeric cell_row/cell_col: '$line'")
              } else {
                classifyNdvi(ndviRaw) match {
                  case Left(ndviError) =>
                    Malformed(s"invalid ndvi field '$ndviRaw': $ndviError")
                  case Right(observation) =>
                    Parsed(ParsedRecord(tile, yearMonthOf(date), observation))
                }
              }
          }
        }
      }
    }
  }

  /**
   * Classifies the raw `ndvi` text field.
   *
   * `Left` is reserved for a field that is neither the missing-value
   * token nor parseable as a number AT ALL -- a genuinely malformed
   * record. An out-of-range-but-parseable number is a legitimate
   * `INVALID` classification (`Right`), never a parse failure -- Task 2B
   * requires invalid values to be preserved and counted, not rejected.
   */
  def classifyNdvi(raw: String): Either[String, NdviObservationWritable] =
    if (raw == MissingNdviToken) {
      Right(NdviObservationWritable.missing())
    } else {
      Try(raw.toDouble).toOption match {
        case None =>
          Left(s"'$raw' is neither the missing-value token ('$MissingNdviToken') nor a parseable number")
        case Some(value) if value >= NdviMin && value <= NdviMax =>
          Right(NdviObservationWritable.valid(value))
        case Some(value) =>
          Right(NdviObservationWritable.invalid(value))
      }
    }
}
