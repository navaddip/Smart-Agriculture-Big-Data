import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import smartagri.ml._
import smartagri.conformal.ConformalPrediction
import smartagri.conformal.ConformalPrediction.{CalibrationExample, TestExample}
import smartagri.decision.DecisionEngine
import smartagri.decision.DecisionEngine.DecisionInput

object ConformalRun {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ConformalRun").master("local[1]")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // All paths below are RELATIVE to the project root -- run this from the project root
    // (e.g. `java -cp ... ConformalRun` executed with the project's top-level folder as the
    // working directory), so it works unchanged on Linux/macOS/Windows. A temp directory (not
    // /tmp, which doesn't exist on Windows) is used for the cached cell-subset file, via Java's
    // portable java.io.tmpdir system property.
    val cellSubsetPath = new java.io.File(System.getProperty("java.io.tmpdir"), "cell_subset.parquet").getPath
    val cellSubsetSize = sys.env.getOrElse("CELL_SUBSET_SIZE", "150").toInt
    val sentinelFullForCells = spark.read.parquet("processed/sentinel2_ndvi.parquet")
    val cellSubset =
      if (new java.io.File(cellSubsetPath).exists()) spark.read.parquet(cellSubsetPath)
      else {
        val subset = sentinelFullForCells.select("tile", "cell_row", "cell_col").distinct()
          .orderBy("tile", "cell_row", "cell_col").limit(cellSubsetSize)
        subset.write.mode("overwrite").parquet(cellSubsetPath)
        spark.read.parquet(cellSubsetPath)
      }
    val sentinelFull = sentinelFullForCells
    val era5Raw = spark.read.parquet("processed/era5_weather.parquet")
    val soilgridsFull = spark.read.parquet("processed/soilgrids.parquet")
    val sentinelRaw = sentinelFull.join(broadcast(cellSubset), Seq("tile","cell_row","cell_col"), "inner")
    val soilgridsRaw = soilgridsFull.join(broadcast(cellSubset), Seq("tile","cell_row","cell_col"), "inner")

    val prepared = MlDataPreparation.prepare(spark, sentinelRaw, era5Raw, soilgridsRaw)

    // 3-way chronological split: FIT / CALIBRATION (both within the original TRAIN period) / TEST (untouched).
    // Calibration cutoff chosen as the midpoint of the train date range so FIT never sees calibration dates.
    val trainDates = prepared.mlTrain.select("date").distinct().orderBy("date").as[String].collect()
    val calibCutoff = trainDates(trainDates.length / 2)
    println(s"Train dates: ${trainDates.length}, calibration cutoff: $calibCutoff")

    val fitDf = prepared.mlTrain.filter(col("date") < calibCutoff)
    val calibDf = prepared.mlTrain.filter(col("date") >= calibCutoff)
    val fitCount = fitDf.count(); val calibCount = calibDf.count(); val testCount = prepared.mlTest.count()
    println(s"FIT=$fitCount CALIBRATION=$calibCount TEST=$testCount")

    val config = MlConfig.default().copy(numTrees = 10, maxDepth = 3)
    val fitWeighted = MlDataPreparation.withClassWeights(fitDf)
    val model = CropStressModel.fit(fitWeighted, prepared.featureColumns, config)
    println("MODEL_FIT_ON_FIT_ONLY_COMPLETE")

    val getProbPositive = udf((v: org.apache.spark.ml.linalg.Vector) => v(1))

    val calibPreds = CropStressModel.predict(model, calibDf)
      .select(col("sample_id"), col(CropStressModel.LabelColumn).as("label"), getProbPositive(col("probability")).as("probPositive"))
      .collect()
    val testPreds = CropStressModel.predict(model, prepared.mlTest)
      .select(col("sample_id"), col(CropStressModel.LabelColumn).as("label"), getProbPositive(col("probability")).as("probPositive"))
      .collect()
    println(s"Calibration predictions: ${calibPreds.length}, Test predictions: ${testPreds.length}")

    val calibExamples = calibPreds.map(r => CalibrationExample(r.getAs[String]("sample_id"), r.getAs[Int]("label"), r.getAs[Double]("probPositive")))
    val testExamples = testPreds.map(r => TestExample(r.getAs[String]("sample_id"), r.getAs[Int]("label"), r.getAs[Double]("probPositive")))

    val alpha = 0.1
    val qhat = ConformalPrediction.calibrate(calibExamples.toSeq, alpha)
    println(s"alpha=$alpha qhat=$qhat")

    val sets = ConformalPrediction.predictionSets(testExamples.toSeq, qhat)
    val coverage = ConformalPrediction.evaluateCoverage(sets)
    println(s"Empirical coverage: ${coverage.empiricalCoverage} (target >= ${1-alpha})")
    println(s"Average prediction-set size: ${coverage.averageSetSize}")
    println(s"Ambiguous (size=2) count: ${coverage.ambiguousCount} of ${coverage.count}")

    // Decision engine on the test set
    val decisionInputs = testExamples.zip(sets).map { case (te, ps) =>
      DecisionInput(te.sampleId, te.probPositive, ps.size)
    }
    val decisions = DecisionEngine.decideAll(decisionInputs.toSeq)
    val actionCounts = decisions.groupBy(_.action).map { case (k, v) => k -> v.size }
    println(s"Decision action counts: $actionCounts")

    // Persist results -- relative paths, project-root-relative (see note above).
    import java.io.{File, PrintWriter}
    new File("results/conformal").mkdirs()
    new File("results/decisions").mkdirs()
    val pw1 = new PrintWriter("results/conformal/coverage_report.csv")
    pw1.println("alpha,qhat,empirical_coverage,average_set_size,ambiguous_count,total_count")
    pw1.println(s"$alpha,$qhat,${coverage.empiricalCoverage},${coverage.averageSetSize},${coverage.ambiguousCount},${coverage.count}")
    pw1.close()

    val pw2 = new PrintWriter("results/decisions/decisions.csv")
    pw2.println("sample_id,predicted_probability,conformal_set_size,action,reason")
    decisionInputs.zip(decisions).foreach { case (di, d) =>
      val reasonEscaped = d.reason.replace("\"", "'")
      val line = di.sampleId + "," + di.predictedProbability + "," + di.conformalSetSize + "," + d.action + ",\"" + reasonEscaped + "\""
      pw2.println(line)
    }
    pw2.close()

    println("Saved: results/conformal/coverage_report.csv, results/decisions/decisions.csv")
    spark.stop()
    println("CONFORMAL_RUN_COMPLETE")
  }
}
