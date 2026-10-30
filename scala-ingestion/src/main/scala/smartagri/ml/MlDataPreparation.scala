package smartagri.ml

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import smartagri.canonical.CanonicalHistoricalDatasetBuilder
import smartagri.features.FeaturePipelineBuilder

/**
 * Builds the Task 10 training/test DataFrames on top of Task 4B's
 * UNMODIFIED `FeaturePipelineBuilder` -- this module never re-derives
 * the target, the split, or the feature list independently; it only
 * adds what Task 10 specifically needs on top: a stable sample identity
 * column and (train-only) class weights.
 */
object MlDataPreparation {

  /**
   * A deterministic sample identity, from the canonical identity
   * columns Task 4B already guarantees are unique
   * (`tile, cell_row, cell_col, date`) -- NEVER a random ID (Task 10's
   * explicit requirement, echoing the same rule this project has used
   * for sensor IDs since Task 6). Task 11 (conformal prediction) needs
   * to align predictions back to samples across train/calibration/test
   * without any risk of a shuffled join losing that alignment -- this
   * column is what makes that possible.
   */
  def withSampleId(df: DataFrame): DataFrame =
    df.withColumn("sample_id", concat_ws("|", col("tile"), col("cell_row"), col("cell_col"), col("date")))

  final case class PreparedData(
      canonicalRows: Long,
      mlTrain: DataFrame,
      mlTest: DataFrame,
      featureColumns: Seq[String],
      pipelineResult: FeaturePipelineBuilder.PipelineResult
  )

  /**
   * Rebuilds Task 3B's canonical historical dataset (unmodified builder)
   * and Task 4B's ML-ready train/test tables (unmodified pipeline) from
   * the three raw processed sources, then adds `sample_id`. This is
   * necessary because, as in every prior task in this project, no
   * intermediate Parquet output persists between sandbox sessions --
   * the SAME regeneration approach already used in Tasks 5/6/9's real-
   * data verification runs, not a new pattern invented for Task 10.
   */
  def prepare(
      spark: SparkSession,
      sentinelRaw: DataFrame,
      era5Raw: DataFrame,
      soilgridsRaw: DataFrame
  ): PreparedData = {
    val (canonical, _) = CanonicalHistoricalDatasetBuilder.build(spark, sentinelRaw, era5Raw, soilgridsRaw)
    val (mlTrainRaw, mlTestRaw, _, _, pipelineResult) = FeaturePipelineBuilder.run(spark, canonical)

    val featureColumns = FeaturePipelineBuilder.approvedFeatureColumns()

    PreparedData(
      canonicalRows = canonical.count(),
      mlTrain = withSampleId(mlTrainRaw),
      mlTest = withSampleId(mlTestRaw),
      featureColumns = featureColumns,
      pipelineResult = pipelineResult
    )
  }

  final case class ClassBalance(positiveCount: Long, negativeCount: Long, positiveRate: Double)

  /** Reports class balance for whatever DataFrame is passed in -- callers must pass train or test
    * explicitly; this function has no notion of which one it's looking at. */
  def classBalance(df: DataFrame): ClassBalance = {
    val positive = df.filter(col("future_stressed") === 1).count()
    val negative = df.filter(col("future_stressed") === 0).count()
    val total = positive + negative
    ClassBalance(positive, negative, if (total > 0) positive.toDouble / total else 0.0)
  }

  /**
   * Adds a `class_weight` column computed via inverse class frequency,
   * from the DataFrame passed in ONLY -- callers must pass TRAINING data
   * (Task 10's explicit "weights derived ONLY from training labels").
   * MLlib's `RandomForestClassifier` has no `class_weight` parameter
   * (unlike scikit-learn); this is the standard workaround -- an
   * explicit instance-weight column consumed via `setWeightCol`.
   */
  def withClassWeights(train: DataFrame): DataFrame = {
    val balance = classBalance(train)
    val total = balance.positiveCount + balance.negativeCount
    val positiveWeight = if (balance.positiveCount > 0) total.toDouble / (2.0 * balance.positiveCount) else 1.0
    val negativeWeight = if (balance.negativeCount > 0) total.toDouble / (2.0 * balance.negativeCount) else 1.0
    train.withColumn(
      "class_weight",
      when(col("future_stressed") === 1, lit(positiveWeight)).otherwise(lit(negativeWeight))
    )
  }
}
