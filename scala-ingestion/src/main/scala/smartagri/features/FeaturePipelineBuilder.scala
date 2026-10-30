package smartagri.features

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.config.AppConfig

/**
 * Orchestrates the Task 4B leakage-safe feature-engineering pipeline
 * end-to-end: canonical historical table in, ML-ready future-stress
 * dataset out.
 *
 * Ordering is deliberate and matches the leakage-safety requirements
 * throughout Task 4A/4B:
 *   1. engineer per-row features (future pairing, lag/rolling, baseline) --
 *      these are pure per-row/per-window computations, safe regardless
 *      of train/test split.
 *   2. filter to eligible prediction instances (has a future observation,
 *      meets the minimum-history requirement).
 *   3. compute the per-row `future_anomaly` (still a pure per-row
 *      formula -- safe).
 *   4. SPLIT chronologically BEFORE any cross-row statistic is computed.
 *   5. compute the target threshold, soil-imputation statistics, and
 *      redundancy correlations from the TRAINING partition only, then
 *      apply the frozen results to both partitions.
 *
 * Step 4 happening before step 5 is the crux of leakage-safety here: no
 * summary statistic (threshold, imputation mean, correlation) is ever
 * computed from a DataFrame that could contain a single test-period row.
 */
object FeaturePipelineBuilder {

  /** The nine ERA5 fields plus soil fields plus engineered fields that make up the approved feature
    * set (Task 4B section G/H), reduced from the initial broad candidate list based on the REAL
    * Spearman correlations measured on the training partition (all reported in the final response,
    * not assumed here):
    *   - swvl1/swvl2/swvl3/swvl4 pairwise correlations ranged 0.944-0.988 -- every pair exceeds the
    *     documented |rho|>0.9 rule, not just one. Keeping any two would still leave a redundant
    *     pair, so only swvl1 (surface layer, most physiologically immediate, and the layer the
    *     project's own weather-equilibrium design already treats as primary) is retained.
    *   - clay_pct vs sand_pct: rho=-0.921, exceeds the rule. clay_pct vs silt_pct (0.619) and
    *     sand_pct vs silt_pct (-0.865) do not. clay_pct is dropped (its water-retention-relevant
    *     information is partially redundant with the separately-retained water_content_fc);
    *     sand_pct and silt_pct are both kept since neither individually exceeds the threshold with
    *     the remaining variable.
    */
  def approvedFeatureColumns(): Seq[String] =
    Seq("ndvi", "ndvi_lag1", "ndvi_change", "ndvi_rolling_mean", "ndvi_rolling_std", "days_since_prior_observation") ++
      Seq("swvl1") ++
      Seq("u10", "v10", "t2m", "d2m", "ssrd", "e", "tp", "pev") ++
      Seq("ph_water", "nitrogen_g_kg", "soil_organic_carbon", "sand_pct", "silt_pct", "water_content_fc") ++
      Seq("baseline_mean_ndvi", "baseline_std_ndvi", "baseline_observation_count", "soil_data_available") ++
      Seq("day_of_year_sin", "day_of_year_cos")

  val IdentityColumns: Seq[String] = Seq("tile", "cell_row", "cell_col", "date")
  val EvaluationOnlyColumns: Seq[String] = Seq("future_date", "future_ndvi", "future_anomaly")

  /**
   * Steps 1-3 above: engineer features, restrict to eligible prediction
   * instances, compute the per-row future anomaly. Does not split, does
   * not choose a threshold, does not impute -- everything from here on
   * is a pure, row-local computation.
   */
  def prepareEligibleInstances(canonical: DataFrame): DataFrame = {
    val engineered = FeatureEngineering.engineerAll(canonical)
    val withFuture = FeatureSplit.withFuturePairingOnly(engineered)
    val withHistory = FeatureSplit.withMinimumHistory(withFuture)
    val withAnomaly = FutureStressTarget.withFutureAnomaly(withHistory)
    FeatureQualityReport.withSoilAvailabilityFlag(withAnomaly)
  }

  /** Selects exactly the approved feature columns plus identity and target -- never the
    * evaluation-only or diagnostic-only columns. This is the literal enforcement of Task 4B section
    * F/N's "future-only columns are not included in the feature vector" requirement. */
  def selectMlReadyColumns(
      df: DataFrame,
      featureColumns: Seq[String],
      provenanceColumns: Seq[String]
  ): DataFrame =
    df.select((IdentityColumns ++ featureColumns ++ Seq("future_stressed") ++ provenanceColumns).map(col): _*)

  def selectEvaluationOnlyColumns(df: DataFrame): DataFrame =
    df.select((IdentityColumns ++ EvaluationOnlyColumns ++ Seq("future_stressed")).map(col): _*)

  def defaultCanonicalPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/canonical_historical.parquet"

  def defaultOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/ml_features"

  def defaultEvaluationOutputPath(config: AppConfig = AppConfig.default): String =
    s"${config.hdfs.processed}/ml_features_eval"

  final case class PipelineResult(
      eligibleRows: Long,
      trainRows: Long,
      testRows: Long,
      straddlingRows: Long,
      distribution: FutureStressTarget.AnomalyDistribution,
      imputationStats: FeatureQualityReport.ImputationStats,
      shortcutBaseline: FeatureQualityReport.ShortcutBaselineMetrics
  )

  /**
   * Runs the full pipeline. Returns the ML-ready train/test DataFrames
   * (features + target, no future-only columns), the evaluation-only
   * train/test DataFrames (future_ndvi/future_date/future_anomaly, keyed
   * identically), and a summary of every measured quantity.
   */
  def run(spark: SparkSession, canonical: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame, PipelineResult) = {
    val eligible = prepareEligibleInstances(canonical)
    val split = FeatureSplit.chronologicalSplit(eligible)

    val distribution = FutureStressTarget.anomalyDistribution(split.train)

    val trainWithTarget = FutureStressTarget.withFutureStressed(split.train, FutureStressTarget.ApprovedThreshold)
    val testWithTarget = FutureStressTarget.withFutureStressed(split.test, FutureStressTarget.ApprovedThreshold)

    val imputationStats = FeatureQualityReport.computeImputationStatistics(trainWithTarget)
    val trainImputed = FeatureQualityReport.applyImputation(trainWithTarget, imputationStats)
    val testImputed = FeatureQualityReport.applyImputation(testWithTarget, imputationStats)

    val shortcutBaseline = FeatureQualityReport.trainShortcutBaseline(trainWithTarget, testWithTarget)

    val featureCols = approvedFeatureColumns()
    val provenanceCols = Seq("sentinel_dedup_applied", "soilgrids_nodata_repaired", "era5_match_distance_km", "weather_available")

    val mlTrain = selectMlReadyColumns(trainImputed, featureCols, provenanceCols)
    val mlTest = selectMlReadyColumns(testImputed, featureCols, provenanceCols)
    val evalTrain = selectEvaluationOnlyColumns(trainWithTarget)
    val evalTest = selectEvaluationOnlyColumns(testWithTarget)

    val result = PipelineResult(
      eligibleRows = eligible.count(),
      trainRows = mlTrain.count(),
      testRows = mlTest.count(),
      straddlingRows = split.straddling.count(),
      distribution = distribution,
      imputationStats = imputationStats,
      shortcutBaseline = shortcutBaseline
    )

    (mlTrain, mlTest, evalTrain, evalTest, result)
  }

  /**
   * Leakage-audit assertions (Task 4B section N). Returns a list of
   * failure messages -- empty means every assertion passed. Never
   * silently ignores a failure.
   */
  def auditLeakage(mlTrain: DataFrame, mlTest: DataFrame, featureCols: Seq[String]): Seq[String] = {
    val failures = scala.collection.mutable.ListBuffer.empty[String]

    // 1 & 2: future_ndvi / future_date never in the feature columns
    if (featureCols.contains("future_ndvi")) failures += "future_ndvi is present in the feature column list"
    if (featureCols.contains("future_date")) failures += "future_date is present in the feature column list"
    if (featureCols.contains("future_anomaly")) failures += "future_anomaly is present in the feature column list"

    // These must also be absent from the actual ML-ready DataFrame's schema, not just the intended list
    Seq("future_ndvi", "future_date", "future_anomaly", "future_gap_days").foreach { forbidden =>
      if (mlTrain.columns.contains(forbidden)) failures += s"ML-ready train table contains forbidden column '$forbidden'"
      if (mlTest.columns.contains(forbidden)) failures += s"ML-ready test table contains forbidden column '$forbidden'"
    }
    Seq("sensor_health", "corruption_type", "corruption_severity").foreach { forbidden =>
      if (mlTrain.columns.contains(forbidden)) failures += s"ML-ready train table contains forbidden column '$forbidden'"
    }

    failures.toSeq
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FeaturePipelineBuilder").getOrCreate()
    try {
      val config = AppConfig.default
      val canonicalPath = if (args.length >= 1) args(0) else defaultCanonicalPath(config)
      val outputPath = if (args.length >= 2) args(1) else defaultOutputPath(config)
      val evalOutputPath = if (args.length >= 3) args(2) else defaultEvaluationOutputPath(config)

      val canonical = spark.read.parquet(canonicalPath)
      val (mlTrain, mlTest, evalTrain, evalTest, result) = run(spark, canonical)

      val featureCols = approvedFeatureColumns()
      val auditFailures = auditLeakage(mlTrain, mlTest, featureCols)
      if (auditFailures.nonEmpty) {
        System.err.println("LEAKAGE AUDIT FAILED:")
        auditFailures.foreach(f => System.err.println(s"  - $f"))
        System.exit(1)
      }

      val mlAll = mlTrain.withColumn("split", org.apache.spark.sql.functions.lit("train"))
        .unionByName(mlTest.withColumn("split", org.apache.spark.sql.functions.lit("test")))
      mlAll.write.mode("overwrite").partitionBy("date").parquet(outputPath)

      val evalAll = evalTrain.withColumn("split", org.apache.spark.sql.functions.lit("train"))
        .unionByName(evalTest.withColumn("split", org.apache.spark.sql.functions.lit("test")))
      evalAll.write.mode("overwrite").partitionBy("date").parquet(evalOutputPath)

      val d = result.distribution
      val report = Seq(
        "eligible_rows" -> result.eligibleRows.toString, "train_rows" -> result.trainRows.toString,
        "test_rows" -> result.testRows.toString, "straddling_rows_excluded" -> result.straddlingRows.toString,
        "split_cutoff" -> FeatureSplit.SplitCutoffDate, "stress_threshold" -> FutureStressTarget.ApprovedThreshold.toString,
        "min_prior_history" -> FeatureSplit.MinimumPriorHistory.toString,
        "train_anomaly_p10" -> d.p10.toString, "train_anomaly_p50" -> d.p50.toString, "train_anomaly_p90" -> d.p90.toString,
        "train_rows_with_imputed_values" -> result.imputationStats.affectedRowCount.toString,
        "test_weather_available_rows" -> mlTest.filter(col("weather_available")).count().toString,
        "train_weather_available_rows" -> mlTrain.filter(col("weather_available")).count().toString,
        "feature_count" -> featureCols.size.toString
      ) ++ result.imputationStats.statistics.toSeq.sortBy(_._1).map { case (k, v) => s"train_mean_$k" -> v.toString }
      import spark.implicits._
      sys.env.get("SMART_AGRI_EXPERIMENT_TAG") match {
        case Some(tag) => smartagri.preprocess.PreprocessSupport.writeReport(spark, report.toDF("key", "value"), "feature_pipeline_summary", s"experiments/$tag")
        case None => smartagri.preprocess.PreprocessSupport.writeReport(spark, report.toDF("key", "value"), "feature_pipeline_summary")
      }

      println(s"ML-ready features written to $outputPath")
      println(s"Evaluation-only table written to $evalOutputPath")
      println(s"Result: $result")
    } finally {
      spark.stop()
    }
  }
}
