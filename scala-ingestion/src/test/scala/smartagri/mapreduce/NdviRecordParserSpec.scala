package smartagri.mapreduce

import org.scalatest.funsuite.AnyFunSuite

import smartagri.model.SentinelObservation

class NdviRecordParserSpec extends AnyFunSuite {

  // -- Constants stay in sync with Task 2A's contract (requirement: never re-decide independently) --

  test("ExpectedFieldCount matches Task 2A's NdviFlatInputPreparation.ExportColumns length") {
    assert(NdviRecordParser.ExpectedFieldCount == NdviFlatInputPreparation.ExportColumns.length)
  }

  test("MissingNdviToken matches Task 2A's NdviFlatInputPreparation.MissingNdviToken") {
    assert(NdviRecordParser.MissingNdviToken == NdviFlatInputPreparation.MissingNdviToken)
  }

  // -- 1. Valid NDVI classification --------------------------------------

  test("classifyNdvi classifies an in-range value as VALID and preserves it") {
    val result = NdviRecordParser.classifyNdvi("0.42")
    result match {
      case Right(obs) =>
        assert(obs.status == NdviStatus.Valid)
        assert(obs.value == 0.42)
      case Left(err) => fail(s"expected Right, got Left($err)")
    }
  }

  // -- 2. Missing NDVI ------------------------------------------------------

  test("classifyNdvi classifies the missing-value token as MISSING") {
    val result = NdviRecordParser.classifyNdvi("NA")
    result match {
      case Right(obs) => assert(obs.status == NdviStatus.Missing)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  // -- 3. Invalid NDVI --------------------------------------------------------

  test("classifyNdvi classifies an out-of-range but parseable value as INVALID, preserving it") {
    val tooHigh = NdviRecordParser.classifyNdvi("1.5")
    val tooLow = NdviRecordParser.classifyNdvi("-2.0")
    tooHigh match {
      case Right(obs) => assert(obs.status == NdviStatus.Invalid); assert(obs.value == 1.5)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
    tooLow match {
      case Right(obs) => assert(obs.status == NdviStatus.Invalid); assert(obs.value == -2.0)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  test("classifyNdvi treats a genuinely non-numeric, non-token field as a parse failure (Left), not INVALID") {
    val result = NdviRecordParser.classifyNdvi("not-a-number")
    assert(result.isLeft)
  }

  // -- 4 & 5. Boundary values ----------------------------------------------------

  test("classifyNdvi treats the exact lower bound (-1.0) as VALID") {
    val result = NdviRecordParser.classifyNdvi(SentinelObservation.NdviMin.toString)
    result match {
      case Right(obs) => assert(obs.status == NdviStatus.Valid)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  test("classifyNdvi treats the exact upper bound (1.0) as VALID") {
    val result = NdviRecordParser.classifyNdvi(SentinelObservation.NdviMax.toString)
    result match {
      case Right(obs) => assert(obs.status == NdviStatus.Valid)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  test("classifyNdvi treats just-below the lower bound as INVALID, not VALID") {
    val justBelow = SentinelObservation.NdviMin - 0.0001
    val result = NdviRecordParser.classifyNdvi(justBelow.toString)
    result match {
      case Right(obs) => assert(obs.status == NdviStatus.Invalid)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  test("classifyNdvi treats just-above the upper bound as INVALID, not VALID") {
    val justAbove = SentinelObservation.NdviMax + 0.0001
    val result = NdviRecordParser.classifyNdvi(justAbove.toString)
    result match {
      case Right(obs) => assert(obs.status == NdviStatus.Invalid)
      case Left(err)  => fail(s"expected Right, got Left($err)")
    }
  }

  // -- 6. Malformed record (wrong field count) -----------------------------------

  test("parseLine rejects a line with too few fields as Malformed") {
    val result = NdviRecordParser.parseLine("T43PHQ\t2023-04-13\t0\t0")
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  test("parseLine rejects a line with too many fields as Malformed") {
    val result = NdviRecordParser.parseLine("T43PHQ\t2023-04-13\t0\t0\t0.42\textra")
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  test("parseLine rejects an empty tile field as Malformed") {
    val result = NdviRecordParser.parseLine("\t2023-04-13\t0\t0\t0.42")
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  test("parseLine rejects non-numeric cell_row/cell_col as Malformed") {
    val result = NdviRecordParser.parseLine("T43PHQ\t2023-04-13\tX\tY\t0.42")
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  test("parseLine rejects null as Malformed rather than throwing") {
    val result = NdviRecordParser.parseLine(null)
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  test("parseLine rejects a tile containing the reserved grouping-key separator ('|') as Malformed") {
    val result = NdviRecordParser.parseLine("T43|PHQ\t2023-04-13\t0\t0\t0.42")
    assert(result.isInstanceOf[NdviRecordParser.Malformed])
  }

  // -- 7. Malformed date --------------------------------------------------------

  test("parseLine rejects an ambiguous/invalid date as Malformed") {
    val unpadded = NdviRecordParser.parseLine("T43PHQ\t2023-4-13\t0\t0\t0.42")
    val wrongFormat = NdviRecordParser.parseLine("T43PHQ\t04/13/2023\t0\t0\t0.42")
    val impossibleDate = NdviRecordParser.parseLine("T43PHQ\t2023-02-30\t0\t0\t0.42")
    assert(unpadded.isInstanceOf[NdviRecordParser.Malformed])
    assert(wrongFormat.isInstanceOf[NdviRecordParser.Malformed])
    assert(impossibleDate.isInstanceOf[NdviRecordParser.Malformed])
  }

  // -- A well-formed line, end to end -------------------------------------------

  test("parseLine correctly parses a well-formed valid-NDVI line") {
    val result = NdviRecordParser.parseLine("T43PHQ\t2023-04-13\t0\t0\t0.42")
    result match {
      case NdviRecordParser.Parsed(record) =>
        assert(record.tile == "T43PHQ")
        assert(record.yearMonth == "2023-04")
        assert(record.observation.status == NdviStatus.Valid)
        assert(record.observation.value == 0.42)
      case NdviRecordParser.Malformed(reason) => fail(s"expected Parsed, got Malformed($reason)")
    }
  }

  test("parseLine correctly parses a well-formed missing-NDVI line") {
    val result = NdviRecordParser.parseLine("T43PHQ\t2023-04-13\t0\t0\tNA")
    result match {
      case NdviRecordParser.Parsed(record) => assert(record.observation.status == NdviStatus.Missing)
      case NdviRecordParser.Malformed(reason) => fail(s"expected Parsed, got Malformed($reason)")
    }
  }

  // -- 8. Mapper key generation --------------------------------------------------

  test("groupingKey produces the exact 'tile|year_month' format") {
    assert(NdviRecordParser.groupingKey("T43PHQ", "2023-04") == "T43PHQ|2023-04")
  }

  test("groupingKey uses the same separator character exposed as GroupingKeySeparator") {
    assert(NdviRecordParser.groupingKey("T43PHQ", "2023-04") ==
      s"T43PHQ${NdviRecordParser.GroupingKeySeparator}2023-04")
  }

  test("yearMonthOf derives YYYY-MM from a CanonicalDate") {
    val date = smartagri.time.CanonicalDate("2023-04-13")
    assert(NdviRecordParser.yearMonthOf(date) == "2023-04")
  }
}
