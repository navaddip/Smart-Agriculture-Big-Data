package smartagri.integration

import org.scalatest.funsuite.AnyFunSuite

import smartagri.kalman.KalmanConfig

class StreamingKalmanAdapterSpec extends AnyFunSuite {

  private val config = KalmanConfig.default

  private def obs(tile: String, cellRow: Int, cellCol: Int, eventTime: String, value: Option[Double]) =
    KalmanObservationInput(tile, cellRow, cellCol, eventTime, value)

  test("a single-cell sequence produces a deterministic Kalman result using the REAL Task 5 filter") {
    val sequence = Seq(
      obs("T1", 0, 0, "2024-01-01T00:00:00Z", Some(0.20)),
      obs("T1", 0, 0, "2024-01-02T00:00:00Z", Some(0.22)),
      obs("T1", 0, 0, "2024-01-03T00:00:00Z", Some(0.25))
    )
    val run1 = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    val run2 = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    assert(run1 == run2, "identical input must produce identical output (determinism)")
    assert(run1.length == 3)
    assert(run1.forall(_.observedValue.isDefined))
    assert(run1.forall(_.observationReliability.isDefined))
    assert(run1.forall(_.measurementVariance.isDefined))
    assert(run1.forall(_.innovation.isDefined))
  }

  test("tile/cellRow/cellCol are preserved unchanged on every output row") {
    val sequence = Seq(obs("T43PHQ", 5, 12, "2024-01-01T00:00:00Z", Some(0.3)))
    val result = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    assert(result.head.tile == "T43PHQ")
    assert(result.head.cellRow == 5)
    assert(result.head.cellCol == 12)
  }

  test("eventTime is preserved verbatim, never replaced by a wall-clock timestamp") {
    val sequence = Seq(obs("T1", 0, 0, "2024-03-15T08:30:00Z", Some(0.3)))
    val result = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    assert(result.head.eventTime == "2024-03-15T08:30:00Z")
  }

  test("a missing observation gets a predict-only step: state carries forward, no update fields are populated") {
    val sequence = Seq(
      obs("T1", 0, 0, "2024-01-01T00:00:00Z", Some(0.25)),
      obs("T1", 0, 0, "2024-01-02T00:00:00Z", None)
    )
    val result = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    val missingRow = result(1)
    assert(missingRow.observedValue.isEmpty)
    assert(missingRow.observationReliability.isEmpty, "reliability must be None, not a fake 0.0, for a missing observation")
    assert(missingRow.measurementVariance.isEmpty)
    assert(missingRow.innovation.isEmpty)
    assert(!missingRow.kalmanState.isNaN)
  }

  test("observations are processed in the given (assumed pre-sorted) order, and later state reflects earlier history") {
    val sequence = Seq(
      obs("T1", 0, 0, "2024-01-01T00:00:00Z", Some(0.20)),
      obs("T1", 0, 0, "2024-01-05T00:00:00Z", Some(0.20)),
      obs("T1", 0, 0, "2024-01-10T00:00:00Z", Some(0.20))
    )
    val result = StreamingKalmanAdapter.processCellSequential(sequence, "soil_moisture", config)
    assert(result.forall(r => r.kalmanVariance >= 0.0 && !r.kalmanVariance.isNaN))
    assert(result(2).observationReliability.get > 0.5, "a value identical to recent history should be highly reliable")
  }

  test("KalmanObservationInput and KalmanStreamOutput have no field capable of representing ground truth or sensor_health") {
    val forbidden = Set("trueValue", "corruptionType", "corruptionSeverity", "sensorHealth")
    val inputFields = classOf[KalmanObservationInput].getDeclaredFields.map(_.getName).toSet
    val outputFields = classOf[KalmanStreamOutput].getDeclaredFields.map(_.getName).toSet
    assert(forbidden.intersect(inputFields).isEmpty, s"KalmanObservationInput must not have any of $forbidden")
    assert(forbidden.intersect(outputFields).isEmpty, s"KalmanStreamOutput must not have any of $forbidden")
  }

  test("CompatibleSensorType is exactly soil_moisture, per the documented sensor-semantics scope") {
    assert(StreamingKalmanAdapter.CompatibleSensorType == "soil_moisture")
  }
}
