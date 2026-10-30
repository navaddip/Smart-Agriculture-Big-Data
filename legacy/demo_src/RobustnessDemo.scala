import smartagri.iot.{SensorSeriesGenerator, IotSimulatorConfig, CorruptionConfig}
import smartagri.kalman.{AdaptiveKalmanFilter, KalmanConfig, KalmanState, ReliabilityEstimator}

object RobustnessDemo {
  def main(args: Array[String]): Unit = {
    val sensorConfig = IotSimulatorConfig.defaultSensorTypes.find(_.name == "soil_moisture").get
    val kalmanConfig = KalmanConfig.default

    val outlierCorruption = CorruptionConfig(0.0, 0.0, 0.0, 0.15, 0.5, 0.0, 0.0, 0.0)
    val missingCorruption = CorruptionConfig(0.20, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    val biasCorruption    = CorruptionConfig(0.0, 0.0, 0.0, 0.0, 0.0, 0.20, 0.15, 0.0)

    val cleanSeries = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, sensorConfig, IotSimulatorConfig.NoCorruption, 60)
    val outlierSeries = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, sensorConfig, outlierCorruption, 60)
    val missingSeries = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, sensorConfig, missingCorruption, 60)
    val biasSeries = SensorSeriesGenerator.generateSeries(42L, 0.25, 0.0, sensorConfig, biasCorruption, 60)

    def runKalman(series: Seq[SensorSeriesGenerator.GeneratedPoint], adaptive: Boolean): (Seq[Double], Seq[Double]) = {
      var state = KalmanState(kalmanConfig.initialStateMean, kalmanConfig.initialStateVariance)
      val priorWindow = scala.collection.mutable.ArrayBuffer.empty[Double]
      val estimates = scala.collection.mutable.ArrayBuffer.empty[Double]
      val reliabilities = scala.collection.mutable.ArrayBuffer.empty[Double]
      series.foreach { p =>
        p.observedValue match {
          case Some(v) =>
            val reliability = if (adaptive) {
              val w = priorWindow.takeRight(kalmanConfig.reliabilityRollingWindowSize).toSeq
              val mean = if (w.nonEmpty) Some(w.sum / w.length) else None
              val std = if (w.length >= 2) {
                val m = w.sum / w.length
                Some(math.sqrt(w.map(x => (x - m) * (x - m)).sum / (w.length - 1)))
              } else None
              ReliabilityEstimator.ndviReliability(v, mean, std, kalmanConfig)
            } else 1.0
            val (_, result) =
              if (adaptive) AdaptiveKalmanFilter.stepAdaptive(state, 1, v, reliability, kalmanConfig)
              else AdaptiveKalmanFilter.stepStatic(state, 1, v, kalmanConfig)
            state = result.posterior
            priorWindow += v
            estimates += state.x
            reliabilities += reliability
          case None =>
            state = AdaptiveKalmanFilter.predict(state, 1, kalmanConfig)
            estimates += state.x
            reliabilities += Double.NaN
        }
      }
      (estimates.toSeq, reliabilities.toSeq)
    }

    def rmse(estimates: Seq[Double], truth: Seq[Double]): Double = {
      val sq = estimates.zip(truth).map { case (e, t) => (e - t) * (e - t) }
      math.sqrt(sq.sum / sq.length)
    }

    def report(name: String, series: Seq[SensorSeriesGenerator.GeneratedPoint]): (Double, Double) = {
      val truth = series.map(_.trueValue)
      val (staticEst, _) = runKalman(series, false)
      val (adaptiveEst, adaptiveRel) = runKalman(series, true)
      val staticRmse = rmse(staticEst, truth)
      val adaptiveRmse = rmse(adaptiveEst, truth)
      println(f"$name%-20s static_RMSE=$staticRmse%.5f adaptive_RMSE=$adaptiveRmse%.5f delta=${adaptiveRmse - staticRmse}%+.5f")
      (staticRmse, adaptiveRmse)
    }

    println("=== PHASE 4/5: Corruption Robustness Demonstration (REAL Task 5/6 code, ground truth used only for evaluation) ===")
    report("clean", cleanSeries)
    report("outlier(15%)", outlierSeries)
    report("missing(20%)", missingSeries)
    report("bias(20%,+0.15)", biasSeries)

    println("\n=== PHASE 3: Mechanism demonstration -- adaptive reliability visibly down-weights a suspicious reading ===")
    val (staticEst, _) = runKalman(outlierSeries, false)
    val (adaptiveEst, adaptiveRel) = runKalman(outlierSeries, true)
    val outlierIndices = outlierSeries.zipWithIndex.filter(_._1.corruptionType.contains("outlier")).map(_._2)
    println(s"Outlier corruption fired at ${outlierIndices.length} of ${outlierSeries.length} points. First 5 shown:")
    outlierIndices.take(5).foreach { i =>
      println(f"  t=$i true=${outlierSeries(i).trueValue}%.3f observed=${outlierSeries(i).observedValue.get}%.3f " +
        f"adaptiveReliability=${adaptiveRel(i)}%.3f staticEstimate=${staticEst(i)}%.3f adaptiveEstimate=${adaptiveEst(i)}%.3f " +
        f"(static moved ${math.abs(staticEst(i) - (if (i>0) staticEst(i-1) else kalmanConfig.initialStateMean))}%.3f, " +
        f"adaptive moved ${math.abs(adaptiveEst(i) - (if (i>0) adaptiveEst(i-1) else kalmanConfig.initialStateMean))}%.3f)")
    }

    // Persist results as CSV for the dashboard.
    // Path is relative to the project root -- run this from the project root (same convention
    // dashboard.py and the other demo drivers use), so it works unchanged on Linux/macOS/Windows.
    import java.io.{File, PrintWriter}
    new File("results/kalman").mkdirs()
    val pw = new PrintWriter("results/kalman/robustness_summary.csv")
    pw.println("scenario,static_rmse,adaptive_rmse,delta")
    Seq(
      "clean" -> report("clean", cleanSeries),
      "outlier_15pct" -> report("outlier_15pct", outlierSeries),
      "missing_20pct" -> report("missing_20pct", missingSeries),
      "bias_20pct" -> report("bias_20pct", biasSeries)
    ).foreach { case (name, (s, a)) => pw.println(s"$name,$s,$a,${a - s}") }
    pw.close()
    println("\nSaved: results/kalman/robustness_summary.csv")
  }
}
