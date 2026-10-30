package smartagri.validation

import org.scalatest.funsuite.AnyFunSuite
import smartagri.model.SensorHealth
import smartagri.spatial.GridCell

class SchemaValidationSpec extends AnyFunSuite {

  // -- GridCell -----------------------------------------------------------

  test("validateGridCell accepts a valid cell") {
    val result = SchemaValidation.validateGridCell("T43PHQ", 1, 2)
    assert(result == Right(GridCell("T43PHQ", 1, 2)))
  }

  test("validateGridCell reports every violation at once for an invalid cell") {
    val result = SchemaValidation.validateGridCell("", -1, -1)
    assert(result.isLeft)
    val errs = result.left.getOrElse(Nil)
    assert(errs.length == 3) // empty tile + negative row + negative col, all reported together
  }

  // -- Date -----------------------------------------------------------------

  test("validateDate accepts a valid ISO date") {
    assert(SchemaValidation.validateDate("2023-04-13").isRight)
  }

  test("validateDate rejects an invalid date without throwing") {
    val result = SchemaValidation.validateDate("2023/04/13")
    assert(result.isLeft)
  }

  // -- NDVI -------------------------------------------------------------------

  test("validateNdvi treats a missing value (None) as valid, not an error") {
    assert(SchemaValidation.validateNdvi(None) == Right(None))
  }

  test("validateNdvi accepts values within [-1, 1]") {
    assert(SchemaValidation.validateNdvi(Some(0.42)) == Right(Some(0.42)))
    assert(SchemaValidation.validateNdvi(Some(-1.0)).isRight)
    assert(SchemaValidation.validateNdvi(Some(1.0)).isRight)
  }

  test("validateNdvi rejects values outside [-1, 1] rather than clipping them") {
    val tooHigh = SchemaValidation.validateNdvi(Some(1.5))
    val tooLow = SchemaValidation.validateNdvi(Some(-2.0))
    assert(tooHigh.isLeft)
    assert(tooLow.isLeft)
    // Not silently clipped to 1.0/-1.0 -- confirm the original out-of-range value is what gets reported.
    assert(tooHigh.left.getOrElse(Nil).exists(_.contains("1.5")))
  }

  // -- IoT ----------------------------------------------------------------

  test("validateIotReading accepts a fully valid reading") {
    val result = SchemaValidation.validateIotReading(
      schemaVersion = 1, sensorId = "sensor-001",
      tile = "T43PHQ", cellRow = 1, cellCol = 2,
      eventTimeIso = "2026-09-02T14:03:11Z",
      soilMoistureVwc = 0.32, soilTemperatureC = 24.5, airTemperatureC = 29.1,
      sensorHealth = "OK"
    )
    assert(result.isRight)
    result match {
      case Right(reading) => assert(reading.sensorHealth == SensorHealth.Ok)
      case Left(errs)      => fail(s"expected a valid reading, got errors: ${errs.mkString(", ")}")
    }
  }

  test("validateIotReading rejects a reading with multiple problems and reports all of them") {
    val result = SchemaValidation.validateIotReading(
      schemaVersion = 0,              // invalid: must be positive
      sensorId = "",                  // invalid: empty
      tile = "T43PHQ", cellRow = 1, cellCol = 2,
      eventTimeIso = "not-a-timestamp", // invalid
      soilMoistureVwc = 1.5,          // invalid: outside [0, 1]
      soilTemperatureC = 24.5,
      airTemperatureC = 29.1,
      sensorHealth = "BROKEN"         // invalid: not OK/DEGRADED
    )
    assert(result.isLeft)
    val errs = result.left.getOrElse(Nil)
    assert(errs.length == 5)
  }

  // -- Canonical key --------------------------------------------------------

  test("validateCanonicalKey succeeds when both spatial and temporal components are valid") {
    val result = SchemaValidation.validateCanonicalKey("T43PHQ", 1, 2, "2023-04-13")
    assert(result.isRight)
  }

  test("validateCanonicalKey reports combined errors when both components are invalid") {
    val result = SchemaValidation.validateCanonicalKey("", -1, -1, "not-a-date")
    assert(result.isLeft)
    assert(result.left.getOrElse(Nil).length == 4) // 3 spatial + 1 temporal
  }
}
