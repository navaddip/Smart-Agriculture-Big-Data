package smartagri.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

/**
 * Builds and trains the Task 10 crop-stress classifier: a plain Spark
 * ML `Pipeline` of `VectorAssembler` -> `RandomForestClassifier`, with
 * TRAIN-ONLY class weights (`MlDataPreparation.withClassWeights`) fed
 * via `setWeightCol` -- MLlib's `RandomForestClassifier` has no
 * scikit-learn-style `class_weight` parameter, so this is the standard,
 * correct workaround, not an omission.
 */
object CropStressModel {

  val LabelColumn: String = "future_stressed"
  val FeaturesVectorColumn: String = "features"
  val WeightColumn: String = "class_weight"

  def buildPipeline(featureColumns: Seq[String], config: MlConfig): Pipeline = {
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns.toArray)
      .setOutputCol(FeaturesVectorColumn)
      // Explicitly NOT relying on "skip" as a data-cleaning mechanism (Task 10's explicit
      // instruction) -- Task 4B's own pipeline already handles missing soil values via imputation
      // + a soil_data_available flag, so by the time data reaches this assembler there should be no
      // null feature vectors to skip; "error" surfaces any unexpected null loudly instead of
      // silently discarding rows.
      .setHandleInvalid("error")

    val rf = new RandomForestClassifier()
      .setLabelCol(LabelColumn)
      .setFeaturesCol(FeaturesVectorColumn)
      .setWeightCol(WeightColumn)
      .setNumTrees(config.numTrees)
      .setMaxDepth(config.maxDepth)
      .setSeed(config.seed)

    new Pipeline().setStages(Array(assembler, rf))
  }

  /** Fits the pipeline on TRAINING data only -- `train` must already have `class_weight`
    * (`MlDataPreparation.withClassWeights`) and the target column present. */
  def fit(train: DataFrame, featureColumns: Seq[String], config: MlConfig): PipelineModel =
    buildPipeline(featureColumns, config).fit(train)

  def predict(model: PipelineModel, data: DataFrame): DataFrame = model.transform(data)

  def save(model: PipelineModel, config: MlConfig): Unit =
    model.write.overwrite().save(config.modelPath)
}
