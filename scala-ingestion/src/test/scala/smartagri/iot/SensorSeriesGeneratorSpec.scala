package smartagri.iot

import org.scalatest.funsuite.AnyFunSuite

class SensorSeriesGeneratorSpec extends AnyFunSuite {

  private val soilMoisture = IotSimulatorConfig.defaultSensorTypes.find(_.name == "soil_moisture").get

  test("the same derived seed produces an identical series") {
    val s1 = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, soilMoisture, IotSimulatorConfig.NoCorruption, 20)
    val s2 = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, soilMoisture, IotSimulatorConfig.NoCorruption, 20)
    assert(s1 == s2)
  }

  test("a different derived seed produces a different series") {
    val s1 = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, soilMoisture, IotSimulatorConfig.NoCorruption, 20)
    val s2 = SensorSeriesGenerator.generateSeries(43L, 0.25, 0.0, soilMoisture, IotSimulatorConfig.NoCorruption, 20)
    assert(s1 != s2)
  }

  test("all generated true and observed values stay within [lowerBound, upperBound] under normal (uncorrupted) generation") {
    val series = SensorSeriesGenerator.generateSeries(1L, 0.25, 0.0, soilMoisture, IotSimulatorConfig.NoCorruption, 500)
    series.foreach { p =>
      assert(p.trueValue >= soilMoisture.lowerBound && p.trueValue <= soilMoisture.upperBound,
        s"trueValue ${p.trueValue} out of bounds")
      p.observedValue.foreach { v =>
        assert(v >= soilMoisture.lowerBound && v <= soilMoisture.upperBound, s"observedValue $v out of bounds")
      }
    }
  }

  test("a high-persistence series shows real autocorrelation: consecutive values are much closer than far-apart values") {
    val highPersistence = soilMoisture.copy(persistence = 0.95, processNoiseStd = 0.005)
    val series = SensorSeriesGenerator.generateSeries(7L, 0.25, 0.0, highPersistence, IotSimulatorConfig.NoCorruption, 200)
    val trueValues = series.map(_.trueValue)

    val consecutiveDiffs = trueValues.sliding(2).map { case Seq(a, b) => math.abs(b - a) }.toSeq
    val meanConsecutiveDiff = consecutiveDiffs.sum / consecutiveDiffs.length

    val farApartDiffs = trueValues.indices.filter(_ + 50 < trueValues.length).map(i => math.abs(trueValues(i + 50) - trueValues(i)))
    val meanFarApartDiff = farApartDiffs.sum / farApartDiffs.length

    assert(meanConsecutiveDiff < meanFarApartDiff,
      s"consecutive-step difference ($meanConsecutiveDiff) should be smaller than far-apart difference ($meanFarApartDiff) under high persistence")
  }

  test("increasing measurementNoiseStd alone (process noise held at 0) changes observed values without changing true values") {
    val noProcessNoise = soilMoisture.copy(processNoiseStd = 0.0, measurementNoiseStd = 0.0)
    val withMeasurementNoise = noProcessNoise.copy(measurementNoiseStd = 0.05)

    val seriesNoNoise = SensorSeriesGenerator.generateSeries(5L, 0.25, 0.0, noProcessNoise, IotSimulatorConfig.NoCorruption, 10)
    val seriesWithMeasNoise = SensorSeriesGenerator.generateSeries(5L, 0.25, 0.0, withMeasurementNoise, IotSimulatorConfig.NoCorruption, 10)

    assert(seriesNoNoise.forall(p => math.abs(p.trueValue - 0.25) < 1e-9))
    assert(seriesWithMeasNoise.forall(p => math.abs(p.trueValue - 0.25) < 1e-9),
      "measurement noise must not leak into the true (process) value")
    assert(seriesNoNoise.map(_.observedValue) != seriesWithMeasNoise.map(_.observedValue))
  }

  test("missing corruption produces None, never Some(0.0)") {
    val alwaysMissing = IotSimulatorConfig.NoCorruption.copy(missingProbability = 1.0)
    val series = SensorSeriesGenerator.generateSeries(1L, 0.25, 0.0, soilMoisture, alwaysMissing, 5)
    assert(series.forall(_.observedValue.isEmpty))
    assert(series.forall(_.corruptionType.contains("missing")))
  }

  test("outlier corruption produces a value that can legitimately exceed physical bounds, with metadata recorded") {
    val alwaysOutlier = IotSimulatorConfig.NoCorruption.copy(outlierProbability = 1.0, outlierMagnitude = 10.0)
    val series = SensorSeriesGenerator.generateSeries(1L, 0.25, 0.0, soilMoisture, alwaysOutlier, 3)
    series.foreach { p =>
      assert(p.corruptionType.contains("outlier"))
      assert(p.observedValue.exists(v => v > soilMoisture.upperBound || v < soilMoisture.lowerBound),
        "an outlier must be allowed to exceed the sensor's normal physical bounds")
    }
  }

  test("bias corruption keeps the observed value within physical bounds (clipped), unlike outlier") {
    val alwaysBias = IotSimulatorConfig.NoCorruption.copy(biasProbability = 1.0, biasMagnitude = 1000.0)
    val series = SensorSeriesGenerator.generateSeries(1L, 0.25, 0.0, soilMoisture, alwaysBias, 3)
    series.foreach { p =>
      assert(p.corruptionType.contains("bias"))
      p.observedValue.foreach(v => assert(v >= soilMoisture.lowerBound && v <= soilMoisture.upperBound))
    }
  }

  test("stale corruption repeats the previous observed value") {
    val staleAfterFirst = IotSimulatorConfig.NoCorruption.copy(staleProbability = 1.0)
    val series = SensorSeriesGenerator.generateSeries(1L, 0.25, 0.0, soilMoisture, staleAfterFirst, 4)
    val firstObserved = series(0).observedValue
    (1 until series.length).foreach { i =>
      assert(series(i).observedValue == firstObserved, s"stale point $i should repeat the first observed value")
    }
  }
}
