package smartagri.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, LogisticRegression, RandomForestClassificationModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.conformal.ConformalPrediction
import smartagri.conformal.ConformalPrediction.CalibrationExample
import smartagri.decision.DecisionEngine
import smartagri.features.FeaturePipelineBuilder
import smartagri.preprocess.PreprocessSupport

/**
 * Trains and evaluates the future-stress model on the full ML feature table in HDFS (YARN job).
 *
 *   ml_features (split=train) --chronological--> FIT (earlier dates) | CALIBRATION (later dates)
 *   ml_features (split=test)  -> TEST (dates >= 2024-04-07, never seen by fit, calibration or thresholds)
 *
 *  1. RandomForest (primary)         fit on FIT, class-weighted
 *  2. Baselines on the IDENTICAL split and feature set (Novelty 5):
 *       - majority class             always predicts the FIT majority label
 *       - naive persistence/threshold "stressed next time if stressed now": z_T = (NDVI_T - baseline)/std < -1
 *       - logistic regression         standardised features, class-weighted
 *  3. Split conformal prediction    qhat from CALIBRATION only, coverage measured on TEST, alpha grid
 *  4. Decision engine               probability + conformal ambiguity -> LowRisk / Monitor / InvestigatePriority
 *
 * Outputs: model + predictions + decisions in HDFS (results/...), CSV reports in results/{ml,conformal,decisions}.
 */
object TrainEvaluateJob {

  val Label: String = CropStressModel.LabelColumn

  /** Confusion-matrix metrics for the positive (stressed) class + threshold-free AUCs from a score column. */
  def binaryMetrics(model: String, df: DataFrame, predCol: String, scoreCol: String): Seq[(String, String, Double)] = {
    val c = df.agg(
      sum(when(col(Label) === 1 && col(predCol) === 1, 1).otherwise(0)).as("tp"),
      sum(when(col(Label) === 0 && col(predCol) === 1, 1).otherwise(0)).as("fp"),
      sum(when(col(Label) === 0 && col(predCol) === 0, 1).otherwise(0)).as("tn"),
      sum(when(col(Label) === 1 && col(predCol) === 0, 1).otherwise(0)).as("fn")).head()
    val Seq(tp, fp, tn, fn) = (0 to 3).map(i => c.getLong(i).toDouble)
    val precision = if (tp + fp > 0) tp / (tp + fp) else 0.0
    val recall = if (tp + fn > 0) tp / (tp + fn) else 0.0
    val specificity = if (tn + fp > 0) tn / (tn + fp) else 0.0
    val eval = new BinaryClassificationEvaluator().setLabelCol(Label).setRawPredictionCol(scoreCol)
    val scored = df.withColumn(Label, col(Label).cast("double"))
    Seq(
      "rows" -> (tp + fp + tn + fn), "accuracy" -> (tp + tn) / (tp + fp + tn + fn),
      "balanced_accuracy" -> (recall + specificity) / 2, "precision_stressed" -> precision,
      "recall_stressed" -> recall, "f1_stressed" -> (if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0),
      "roc_auc" -> eval.setMetricName("areaUnderROC").evaluate(scored),
      "pr_auc" -> eval.setMetricName("areaUnderPR").evaluate(scored),
      "tp" -> tp, "fp" -> fp, "tn" -> tn, "fn" -> fn
    ).map { case (k, v) => (model, k, v) }
  }

  def main(args: Array[String]): Unit = {
    val opts = PreprocessSupport.parseArgs(args)
    val featuresPath = opts.getOrElse("features", s"${PreprocessSupport.ProcessedDir}/ml_features")
    // --tag <name>: a labelled side experiment. Everything is written under results/experiments/<name>/
    // so the main results (and the model the streaming job uses) are never overwritten.
    val tag = opts.get("tag")
    def area(a: String): String = tag.map(t => s"experiments/$t/$a").getOrElse(a)
    val resultsHdfs = s"${PreprocessSupport.HdfsBase}/results" + tag.map(t => s"/experiments/$t").getOrElse("")
    val calibrationFraction = opts.getOrElse("calibration-fraction", "0.25").toDouble

    val spark = SparkSession.builder().appName("CropStressTrainEvaluate").getOrCreate()
    import spark.implicits._
    try {
      val featureCols = FeaturePipelineBuilder.approvedFeatureColumns()
      val all = MlDataPreparation.withSampleId(spark.read.parquet(featuresPath))
      val anyNull = featureCols.map(c => col(c).isNull).reduce(_ || _)
      val nullRows = all.filter(anyNull).count()
      // Current anomaly z_T = (NDVI_T - frozen baseline) / std: uses only information available at T.
      // It is the quantity the naive persistence rule thresholds, handed to the tree models directly.
      val data = all.filter(!anyNull && col(Label).isNotNull)
        .withColumn("ndvi_anomaly_now", when($"baseline_std_ndvi" > 0,
          ($"ndvi" - $"baseline_mean_ndvi") / $"baseline_std_ndvi").otherwise(0.0))
        .cache()
      println(s"Rows with a null feature (dropped, reported): $nullRows")

      val train = data.filter($"split" === "train")
      val test = data.filter($"split" === "test").cache()
      val trainDates = train.select("date").distinct().orderBy("date").as[String].collect()
      val calibCutoff = trainDates((trainDates.length * (1 - calibrationFraction)).toInt)
      val fit = MlDataPreparation.withClassWeights(train.filter($"date" < calibCutoff)).cache()
      val calib = train.filter($"date" >= calibCutoff).cache()
      val (nFit, nCal, nTest) = (fit.count(), calib.count(), test.count())
      println(s"FIT=$nFit (dates < $calibCutoff)  CALIBRATION=$nCal  TEST=$nTest")
      val balance = Seq("fit" -> fit, "calibration" -> calib, "test" -> test).map { case (n, df) =>
        val b = MlDataPreparation.classBalance(df); (n, df.select("date").distinct().count(), b.positiveCount, b.negativeCount, b.positiveRate)
      }.toDF("partition", "dates", "stressed", "not_stressed", "stressed_rate")
      balance.show(truncate = false)

      val probPos = udf((v: org.apache.spark.ml.linalg.Vector) => v(1))

      // 1. Random Forest
      val config = { val c = MlConfig.default(); tag.map(t => c.copy(modelPath = c.modelPath + "_" + t)).getOrElse(c) }
      val rf = PreprocessSupport.timed(s"RandomForest fit (${config.numTrees} trees, depth ${config.maxDepth})") {
        CropStressModel.fit(fit, featureCols, config)
      }
      val rfTest = rf.transform(test).withColumn("score", probPos($"probability")).cache()

      // 1b. Enhanced models: constant columns dropped, current anomaly added. Hyper-parameters AND the
      //     choice of primary model are decided on a validation date INSIDE the fit period
      //     (the last fit date) -- the calibration and test periods are never consulted.
      val constantCols = featureCols.filter(c => fit.select(c).distinct().limit(2).count() < 2)
      val enhancedCols = featureCols.filterNot(constantCols.contains) :+ "ndvi_anomaly_now"
      println(s"Constant feature columns dropped: ${constantCols.mkString(", ")}")
      val fitDates = fit.select("date").distinct().orderBy("date").as[String].collect()
      val valCutoff = fitDates.last
      val innerFit = fit.filter($"date" < valCutoff).cache()
      val innerVal = fit.filter($"date" >= valCutoff).cache()
      val aucEval = new BinaryClassificationEvaluator().setLabelCol(Label).setRawPredictionCol("score").setMetricName("areaUnderROC")
      def valAuc(m: PipelineModel): Double =
        aucEval.evaluate(m.transform(innerVal).withColumn("score", probPos($"probability")).withColumn(Label, col(Label).cast("double")))
      def gbt(depth: Int, iters: Int): Pipeline = new Pipeline().setStages(Array(
        new VectorAssembler().setInputCols(enhancedCols.toArray).setOutputCol("features"),
        new GBTClassifier().setLabelCol(Label).setFeaturesCol("features").setWeightCol("class_weight")
          .setMaxDepth(depth).setMaxIter(iters).setStepSize(0.1).setSubsamplingRate(0.8).setSeed(config.seed)))
      val tuning = PreprocessSupport.timed("GBT tuning on the inner validation date") {
        for (d <- Seq(3, 5); n <- Seq(40, 80)) yield (d, n, valAuc(gbt(d, n).fit(innerFit)))
      }
      val (bestDepth, bestIters, gbtValAuc) = tuning.maxBy(_._3)
      val rfEnhValAuc = valAuc(CropStressModel.fit(innerFit, enhancedCols, config))
      val tuningDf = (tuning.map { case (d, n, a) => ("gradient_boosting", s"maxDepth=$d maxIter=$n", a) } :+
        (("random_forest_enhanced", s"numTrees=${config.numTrees} maxDepth=${config.maxDepth}", rfEnhValAuc)))
        .toDF("model", "hyperparameters", "validation_roc_auc").orderBy(desc("validation_roc_auc"))
        .withColumn("validation_date", lit(valCutoff))
      tuningDf.show(truncate = false)
      PreprocessSupport.writeReport(spark, tuningDf, "model_selection", area("ml"))

      val gbtModel = PreprocessSupport.timed(s"GBT fit (depth $bestDepth, $bestIters iterations)")(gbt(bestDepth, bestIters).fit(fit))
      val rfEnh = CropStressModel.fit(fit, enhancedCols, config)
      val gbtTest = gbtModel.transform(test).withColumn("score", probPos($"probability")).cache()
      val rfEnhTest = rfEnh.transform(test).withColumn("score", probPos($"probability")).cache()
      val (primaryName, primaryModel, primaryTest) =
        if (gbtValAuc >= rfEnhValAuc) ("gradient_boosting", gbtModel, gbtTest) else ("random_forest_enhanced", rfEnh, rfEnhTest)
      println(s"Primary model (chosen on validation, not test): $primaryName")
      CropStressModel.save(primaryModel, config)
      val primaryCal = primaryModel.transform(calib).withColumn("score", probPos($"probability"))

      // 2. Baselines -- same FIT rows, same features, same TEST rows
      val majority = if (MlDataPreparation.classBalance(fit).positiveRate >= 0.5) 1 else 0
      val majorityTest = test.withColumn("prediction", lit(majority.toDouble)).withColumn("score", lit(majority.toDouble))
      val z = ($"ndvi" - $"baseline_mean_ndvi") / $"baseline_std_ndvi"
      val naiveTest = test.withColumn("score", -z)   // lower current anomaly -> higher stress score
        .withColumn("prediction", when(z < -1.0, 1.0).otherwise(0.0))
      val lr = new Pipeline().setStages(Array(
        new VectorAssembler().setInputCols(featureCols.toArray).setOutputCol("raw_features"),
        new StandardScaler().setInputCol("raw_features").setOutputCol("features").setWithMean(true),
        new LogisticRegression().setLabelCol(Label).setFeaturesCol("features").setWeightCol("class_weight")
          .setMaxIter(100).setRegParam(0.01)))
        .fit(fit)
      val lrTest = lr.transform(test).withColumn("score", probPos($"probability"))

      val comparison = (
        binaryMetrics("gradient_boosting", gbtTest, "prediction", "score") ++
          binaryMetrics("random_forest_enhanced", rfEnhTest, "prediction", "score") ++
          binaryMetrics("random_forest", rfTest, "prediction", "score") ++
          binaryMetrics("logistic_regression", lrTest, "prediction", "score") ++
          binaryMetrics("naive_persistence_threshold", naiveTest, "prediction", "score") ++
          binaryMetrics("majority_class", majorityTest, "prediction", "score")
        ).toDF("model", "metric", "value")
      val wide = comparison.groupBy("model").pivot("metric").agg(first("value"))
        .select("model", "rows", "accuracy", "balanced_accuracy", "precision_stressed", "recall_stressed",
          "f1_stressed", "roc_auc", "pr_auc", "tp", "fp", "tn", "fn")
        .orderBy(desc("roc_auc"))
      wide.show(truncate = false)
      PreprocessSupport.writeReport(spark, wide, "baseline_comparison", area("ml"))
      PreprocessSupport.writeReport(spark, comparison.filter($"model" === primaryName).select("metric", "value"), "metrics", area("ml"))

      val importanceVector = primaryModel.stages.collectFirst {
        case m: GBTClassificationModel => m.featureImportances
        case m: RandomForestClassificationModel => m.featureImportances
      }.get
      val importances = enhancedCols.zipWithIndex.map { case (c, i) => (c, importanceVector(i)) }.sortBy(-_._2)
        .toDF("feature", "importance")
      importances.show(30, truncate = false)
      PreprocessSupport.writeReport(spark, importances, "feature_importances", area("ml"))
      PreprocessSupport.writeReport(spark, balance, "class_balance", area("ml"))
      PreprocessSupport.writeReport(spark, Seq(
        ("seed", config.seed.toString), ("num_trees", config.numTrees.toString), ("max_depth", config.maxDepth.toString),
        ("fit_rows", nFit.toString), ("calibration_rows", nCal.toString), ("test_rows", nTest.toString),
        ("calibration_cutoff", calibCutoff), ("rows_dropped_null_feature", nullRows.toString),
        ("feature_count", enhancedCols.size.toString), ("primary_model", primaryName),
        ("primary_hyperparameters", if (primaryName == "gradient_boosting") s"maxDepth=$bestDepth maxIter=$bestIters" else "see num_trees/max_depth"),
        ("constant_columns_dropped", constantCols.mkString("|")), ("model_selection_validation_date", valCutoff), ("model_path", config.modelPath),
        ("scale_note", "full dataset -- every eligible cell/date from the HDFS feature table")
      ).toDF("key", "value"), "run_metadata", area("ml"))

      // 3. Split conformal prediction: qhat from CALIBRATION only
      val calScores = primaryCal.select(col(Label), $"score").as[(Int, Double)].collect()
        .zipWithIndex.map { case ((y, p), i) => CalibrationExample(i.toString, y, p) }.toSeq
      val coverage = Seq(0.05, 0.10, 0.20).map { alpha =>
        val qhat = ConformalPrediction.calibrate(calScores, alpha)
        val sets = primaryTest
          .withColumn("inc_neg", ($"score") <= qhat)          // 1 - P(y=0) = p
          .withColumn("inc_pos", (lit(1.0) - $"score") <= qhat)
          .withColumn("set_size", $"inc_neg".cast("int") + $"inc_pos".cast("int"))
          .withColumn("covered", when(col(Label) === 1, $"inc_pos").otherwise($"inc_neg"))
        val r = sets.agg(avg($"covered".cast("double")), avg($"set_size"),
          sum(when($"set_size" === 2, 1).otherwise(0)), sum(when($"set_size" === 0, 1).otherwise(0)),
          avg(when(col(Label) === 1, $"covered".cast("double"))), avg(when(col(Label) === 0, $"covered".cast("double")))).head()
        (alpha, qhat, 1 - alpha, r.getDouble(0), r.getDouble(1), r.getLong(2), r.getLong(3), r.getDouble(4), r.getDouble(5), nCal, nTest)
      }.toDF("alpha", "qhat", "target_coverage", "empirical_coverage", "average_set_size", "ambiguous_count",
        "empty_set_count", "coverage_stressed", "coverage_not_stressed", "calibration_rows", "test_rows")
      coverage.show(truncate = false)
      PreprocessSupport.writeReport(spark, coverage, "coverage_report", area("conformal"))

      // 4. Decisions at alpha = 0.10
      val qhat10 = ConformalPrediction.calibrate(calScores, 0.10)
      val decide = udf { (id: String, p: Double, size: Int) =>
        val d = DecisionEngine.decide(DecisionEngine.DecisionInput(id, p, size))
        (d.action.toString, d.reason)
      }
      val decisions = primaryTest
        .withColumn("conformal_set_size", ($"score" <= qhat10).cast("int") + ((lit(1.0) - $"score") <= qhat10).cast("int"))
        .withColumn("d", decide($"sample_id", $"score", $"conformal_set_size"))
        .select($"sample_id", $"tile", $"cell_row", $"cell_col", $"date", $"score".as("predicted_probability"),
          $"conformal_set_size", $"d._1".as("action"), $"d._2".as("reason"), col(Label).as("actual_future_stressed"))
        .cache()
      decisions.write.mode("overwrite").parquet(s"$resultsHdfs/decisions/decisions.parquet")
      val actionCounts = decisions.groupBy("action").agg(count(lit(1)).as("count"),
        avg($"actual_future_stressed".cast("double")).as("observed_stress_rate")).orderBy("action")
      actionCounts.show(truncate = false)
      PreprocessSupport.writeReport(spark, actionCounts, "decision_summary", area("decisions"))
      PreprocessSupport.writeReport(spark, decisions.orderBy(desc("predicted_probability")).limit(5000), "decisions", area("decisions"))

      // Latest per-cell stress probability -> static context for the streaming job
      primaryTest.select($"tile", $"cell_row", $"cell_col", $"date", $"score".as("stress_probability"), $"ndvi")
        .write.mode("overwrite").parquet(s"$resultsHdfs/predictions/rf_test_predictions.parquet")
      println(s"Model saved to ${config.modelPath}")
    } finally spark.stop()
  }
}
