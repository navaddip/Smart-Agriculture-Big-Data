package smartagri.validation

import smartagri.model.{CanonicalKey, IotSensorReading, SensorHealth, SentinelObservation}
import smartagri.spatial.GridCell
import smartagri.time.{CanonicalDate, IotEventTime}

/**
 * Validation utilities for the canonical models (Task 1H).
 *
 * These validators check STRUCTURE and PHYSICAL PLAUSIBILITY only. They
 * never silently replace, clip, or coerce an invalid value -- an invalid
 * value is always reported as an error string, and a missing (`None`)
 * value is always treated as legitimately absent, never as an error,
 * because the project's Veracity handling downstream depends on knowing
 * what is actually missing versus what is actually wrong (Task 1H is
 * explicit on this point).
 *
 * INTENDED CALL SITE (Task 1 correction pass, Correction 4):
 * `validateIotReading` is designed to sit at exactly one boundary -- the
 * future Kafka ingestion layer's raw-event -> parse -> validate ->
 * trusted-`IotSensorReading` pipeline (see
 * `docs/migration/legacy-compatibility-mapping.md`) -- not to be called
 * redundantly by every downstream module. Fusion/ML/conformal/decision
 * code should consume already-validated `IotSensorReading` values, not
 * re-validate them.
 */
object SchemaValidation {

  /** Zero or more error messages. Empty = valid. */
  type ValidationErrors = List[String]

  // ---------------------------------------------------------------------
  // Spatial
  // ---------------------------------------------------------------------

  /** tile non-empty; cell_row >= 0; cell_col >= 0 (Task 1H). */
  def validateGridCell(tile: String, cellRow: Int, cellCol: Int): Either[ValidationErrors, GridCell] = {
    val errors = List.newBuilder[String]
    if (tile == null || tile.trim.isEmpty) errors += "tile must not be empty"
    if (cellRow < 0) errors += s"cell_row must be >= 0, was $cellRow"
    if (cellCol < 0) errors += s"cell_col must be >= 0, was $cellCol"
    val errs = errors.result()
    if (errs.nonEmpty) Left(errs) else Right(GridCell(tile, cellRow, cellCol))
  }

  // ---------------------------------------------------------------------
  // Temporal
  // ---------------------------------------------------------------------

  /** Valid, strict ISO-8601 YYYY-MM-DD (Task 1H). */
  def validateDate(isoString: String): Either[ValidationErrors, CanonicalDate] =
    CanonicalDate.parse(isoString).left.map(msg => List(msg))

  /** Valid ISO-8601 UTC instant for an IoT event timestamp. */
  def validateEventTime(isoInstant: String): Either[ValidationErrors, IotEventTime] =
    IotEventTime.parse(isoInstant).left.map(msg => List(msg))

  // ---------------------------------------------------------------------
  // NDVI
  // ---------------------------------------------------------------------

  /**
   * `None` is always valid (missing data -- e.g. a nodata/cloud-masked
   * pixel block). A present value must fall in the physically valid
   * range [-1, 1]; anything outside that range is reported as an error
   * rather than silently clipped, since a silently clipped NDVI would
   * hide a real upstream data problem.
   */
  def validateNdvi(ndvi: Option[Double]): Either[ValidationErrors, Option[Double]] = ndvi match {
    case None => Right(None)
    case Some(v) if v >= SentinelObservation.NdviMin && v <= SentinelObservation.NdviMax => Right(Some(v))
    case Some(v) =>
      Left(List(s"ndvi=$v is outside the physically valid range [${SentinelObservation.NdviMin}, ${SentinelObservation.NdviMax}]"))
  }

  // ---------------------------------------------------------------------
  // IoT
  // ---------------------------------------------------------------------

  /**
   * Validates every field of a raw IoT reading and, only if all checks
   * pass, constructs the typed `IotSensorReading`. Collects ALL
   * violations rather than stopping at the first one, so a bad message
   * can be diagnosed in a single pass instead of one error at a time.
   */
  def validateIotReading(
      schemaVersion: Int,
      sensorId: String,
      tile: String,
      cellRow: Int,
      cellCol: Int,
      eventTimeIso: String,
      soilMoistureVwc: Double,
      soilTemperatureC: Double,
      airTemperatureC: Double,
      sensorHealth: String
  ): Either[ValidationErrors, IotSensorReading] = {
    val errors = List.newBuilder[String]

    if (schemaVersion <= 0) errors += s"schema_version must be positive, was $schemaVersion"
    if (sensorId == null || sensorId.trim.isEmpty) errors += "sensor_id must not be empty"

    val cellE = validateGridCell(tile, cellRow, cellCol)
    cellE.left.foreach(errs => errors ++= errs)

    val timeE = validateEventTime(eventTimeIso)
    timeE.left.foreach(errs => errors ++= errs)

    val (minVwc, maxVwc) = IotSensorReading.SoilMoistureVwcRange
    if (soilMoistureVwc.isNaN || soilMoistureVwc < minVwc || soilMoistureVwc > maxVwc)
      errors += s"soil_moisture_vwc=$soilMoistureVwc is outside the physically valid range [$minVwc, $maxVwc]"

    if (soilTemperatureC.isNaN) errors += "soil_temperature_c must not be NaN"
    if (airTemperatureC.isNaN) errors += "air_temperature_c must not be NaN"

    val healthE = SensorHealth.parse(sensorHealth)
    healthE.left.foreach(msg => errors += msg)

    val errs = errors.result()
    if (errs.nonEmpty) Left(errs)
    else
      // Pattern-matched rather than `.toOption.get`, deliberately: `Either#toOption` is a
      // Scala 2.13+ convenience that isn't available on the plain (non-right-biased) `Either`
      // this project's Scala 2.12.18 target provides, so pattern matching keeps this portable.
      (cellE, timeE, healthE) match {
        case (Right(cell), Right(eventTime), Right(health)) =>
          Right(
            IotSensorReading(
              schemaVersion = schemaVersion,
              sensorId = sensorId,
              cell = cell,
              eventTime = eventTime,
              soilMoistureVwc = soilMoistureVwc,
              soilTemperatureC = soilTemperatureC,
              airTemperatureC = airTemperatureC,
              sensorHealth = health
            )
          )
        case _ =>
          // Unreachable given `errs.isEmpty` above (every Either was already confirmed Right),
          // but handled explicitly rather than assumed, per Task 1: "useful errors", not a crash.
          Left(List("internal validation inconsistency: all checks reported valid but a value was still Left"))
      }
  }

  // ---------------------------------------------------------------------
  // Canonical key
  // ---------------------------------------------------------------------

  /** Validates a spatial+temporal identity together and builds the canonical join key. */
  def validateCanonicalKey(tile: String, cellRow: Int, cellCol: Int, date: String): Either[ValidationErrors, CanonicalKey] = {
    val cellE = validateGridCell(tile, cellRow, cellCol)
    val dateE = validateDate(date)
    (cellE, dateE) match {
      case (Right(cell), Right(d)) => Right(CanonicalKey(cell, d))
      case _ =>
        val errors = List.newBuilder[String]
        cellE.left.foreach(errs => errors ++= errs)
        dateE.left.foreach(errs => errors ++= errs)
        Left(errors.result())
    }
  }
}
