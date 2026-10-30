package smartagri.features

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Training-only statistics: soil imputation, redundancy correlation, and
 * the shortcut-baseline diagnostic required by Task 4A (section G) and
 * Task 4B (section M). Every statistic here is fit from a DataFrame the
 * caller must supply as the TRAINING partition -- these functions have
 * no notion of "train" vs "test" themselves; they trust what they're
 * given, exactly like `FutureStressTarget.classBalanceAtThresholds`.
 */
object FeatureQualityReport {

  val SoilColumns: Seq[String] = Seq(
    "ph_water", "nitrogen_g_kg", "soil_organic_carbon", "clay_pct", "sand_pct", "silt_pct", "water_content_fc"
  )

  /** ERA5 feature columns. Missing when a Sentinel date falls outside the ERA5 download's time
    * span (e.g. ERA5 downloaded for 2023 only). Imputed exactly like soil: TRAIN-partition mean,
    * frozen, applied to both partitions -- and the canonical `weather_available` flag records it. */
  val WeatherColumns: Seq[String] = Seq("swvl1", "u10", "v10", "t2m", "d2m", "ssrd", "e", "tp", "pev")

  /** Every column the frozen train-mean imputation covers. */
  val ImputedColumns: Seq[String] = SoilColumns ++ WeatherColumns

  /** Adds `soil_data_available` (1 if ALL required soil columns are present, else 0) -- Task 4B section I. */
  def withSoilAvailabilityFlag(df: DataFrame): DataFrame = {
    val allPresent = SoilColumns.map(c => col(c).isNotNull).reduce(_ && _)
    df.withColumn("soil_data_available", when(allPresent, 1).otherwise(0))
  }

  final case class ImputationStats(statistics: Map[String, Double], affectedRowCount: Long)

  /** Computes the per-column mean from `train` ONLY (Task 4B section I / L) -- never call this with
    * anything but the training partition. */
  def computeImputationStatistics(train: DataFrame): ImputationStats = {
    val means = ImputedColumns.filter(train.columns.contains).map { c =>
      val row = train.agg(avg(col(c))).head()
      c -> (if (row.isNullAt(0)) 0.0 else row.getDouble(0))
    }.toMap
    val affected = train.filter(ImputedColumns.filter(train.columns.contains).map(c => col(c).isNull).reduce(_ || _)).count()
    ImputationStats(means, affected)
  }

  /** Applies FROZEN imputation statistics (from `computeImputationStatistics` on train) to any
    * DataFrame -- train, test, or otherwise. The statistics themselves are the only thing that must
    * come from train; applying them to test rows is the whole point. */
  def applyImputation(df: DataFrame, stats: ImputationStats): DataFrame =
    stats.statistics.keys.toSeq.sorted.foldLeft(df) { (acc, c) =>
      acc.withColumn(c, when(col(c).isNull, lit(stats.statistics(c))).otherwise(col(c)))
    }

  /**
   * Spearman correlation matrix over the given columns, computed from
   * `train` ONLY (Task 4B section J). Rows with any null among the
   * given columns are excluded from the correlation (a correlation
   * computed by silently zero-filling nulls would misrepresent the
   * actual relationship).
   */
  def spearmanCorrelation(train: DataFrame, columns: Seq[String]): Map[(String, String), Double] = {
    val complete = train.select(columns.map(col): _*).na.drop()
    val assembler = new VectorAssembler().setInputCols(columns.toArray).setOutputCol("_corr_features")
    val vectorized = assembler.transform(complete)
    val Row(matrix: Matrix) = Correlation.corr(vectorized, "_corr_features", "spearman").head()

    (for {
      i <- columns.indices
      j <- columns.indices
      if i < j
    } yield (columns(i), columns(j)) -> matrix(i, j)).toMap
  }

  final case class ShortcutBaselineMetrics(
      trainRows: Long,
      testRows: Long,
      accuracy: Double,
      precision: Double,
      recall: Double,
      f1: Double,
      rocAuc: Option[Double],
      prAuc: Option[Double],
      confusionMatrix: Array[Array[Double]]
  )

  /**
   * Trains the required shortcut-detection baseline (Task 4A section G,
   * Task 4B section M) using ONLY `baseline_mean_ndvi`,
   * `baseline_std_ndvi`, and the realized T-to-future gap length --
   * deliberately excluding `ndvi(T)`, weather, soil, and every
   * engineered trend feature.
   *
   * `future_gap_days` (the realized gap between T and its actual T+1) is
   * used HERE ONLY, as an explicit, approved exception (Task 4B section
   * M names it directly) -- it is a mildly leaky quantity at true
   * prediction time (you don't know in advance exactly when the next
   * usable Sentinel pass will occur), which is precisely why this
   * diagnostic model exists: to check whether the target is trivially
   * predictable from signals like this, not to endorse using it in the
   * production feature set. It must never appear in the main
   * leakage-safe feature table (see `FeatureQualityReport`'s callers).
   */
  def trainShortcutBaseline(train: DataFrame, test: DataFrame): ShortcutBaselineMetrics = {
    def prepare(df: DataFrame): DataFrame =
      df
        .withColumn("future_gap_days", datediff(to_date(col("future_date")), to_date(col("date"))))
        .filter(col("future_stressed").isNotNull && col("baseline_mean_ndvi").isNotNull && col("baseline_std_ndvi").isNotNull)

    val trainPrepared = prepare(train)
    val testPrepared = prepare(test)

    val assembler = new VectorAssembler()
      .setInputCols(Array("baseline_mean_ndvi", "baseline_std_ndvi", "future_gap_days"))
      .setOutputCol("features")

    val trainVectorized = assembler.transform(trainPrepared).select(col("features"), col("future_stressed").cast("double").as("label"))
    val testVectorized = assembler.transform(testPrepared).select(col("features"), col("future_stressed").cast("double").as("label"))

    val lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("label")
    val model = lr.fit(trainVectorized)
    val predictions = model.transform(testVectorized).cache()

    val multiEval = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val accuracy = multiEval.setMetricName("accuracy").evaluate(predictions)
    val precision = multiEval.setMetricName("weightedPrecision").evaluate(predictions)
    val recall = multiEval.setMetricName("weightedRecall").evaluate(predictions)
    val f1 = multiEval.setMetricName("f1").evaluate(predictions)

    val distinctLabels = predictions.select("label").distinct().count()
    val (rocAuc, prAuc) =
      if (distinctLabels >= 2) {
        val binEval = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
        val roc = binEval.setMetricName("areaUnderROC").evaluate(predictions)
        val pr = binEval.setMetricName("areaUnderPR").evaluate(predictions)
        (Some(roc), Some(pr))
      } else (None, None) // ROC/PR-AUC are undefined with only one class present in the evaluation set

    val confusion = predictions
      .groupBy("label", "prediction")
      .count()
      .collect()
      .foldLeft(Array.ofDim[Double](2, 2)) { (m, row) =>
        val l = row.getAs[Double]("label").toInt
        val p = row.getAs[Double]("prediction").toInt
        if (l >= 0 && l < 2 && p >= 0 && p < 2) m(l)(p) = row.getAs[Long]("count").toDouble
        m
      }

    ShortcutBaselineMetrics(
      trainRows = trainVectorized.count(),
      testRows = testVectorized.count(),
      accuracy = accuracy,
      precision = precision,
      recall = recall,
      f1 = f1,
      rocAuc = rocAuc,
      prAuc = prAuc,
      confusionMatrix = confusion
    )
  }
}
