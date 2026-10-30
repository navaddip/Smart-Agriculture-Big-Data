package smartagri.ml

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Computes the full evaluation-metric suite Task 10 requires -- never
 * accuracy alone, per its explicit instruction, and with particular
 * attention to positive-class recall/F1/PR-AUC given Task 4B's own
 * documented train/test distribution shift.
 */
object ModelEvaluation {

  final case class Metrics(
      accuracy: Double,
      precision: Double,
      recall: Double,
      f1: Double,
      rocAuc: Double,
      prAuc: Double,
      confusionMatrix: Array[Array[Long]]
  )

  def evaluate(predictions: DataFrame): Metrics = {
    val labelCol = CropStressModel.LabelColumn

    val multiEval = new MulticlassClassificationEvaluator().setLabelCol(labelCol).setPredictionCol("prediction")
    val accuracy = multiEval.setMetricName("accuracy").evaluate(predictions)
    val precision = multiEval.setMetricName("weightedPrecision").evaluate(predictions)
    val recall = multiEval.setMetricName("weightedRecall").evaluate(predictions)
    val f1 = multiEval.setMetricName("f1").evaluate(predictions)

    val binEval = new BinaryClassificationEvaluator().setLabelCol(labelCol).setRawPredictionCol("rawPrediction")
    val rocAuc = binEval.setMetricName("areaUnderROC").evaluate(predictions)
    val prAuc = binEval.setMetricName("areaUnderPR").evaluate(predictions)

    val confusionRows = predictions
      .groupBy(col(labelCol).cast("int").as("actual"), col("prediction").cast("int").as("predicted"))
      .count()
      .collect()

    val confusion = Array.ofDim[Long](2, 2)
    confusionRows.foreach { row =>
      val a = row.getAs[Int]("actual")
      val p = row.getAs[Int]("predicted")
      if (a >= 0 && a < 2 && p >= 0 && p < 2) confusion(a)(p) = row.getAs[Long]("count")
    }

    Metrics(accuracy, precision, recall, f1, rocAuc, prAuc, confusion)
  }

  /** Positive-class-specific recall (Task 10's explicit ask, given the train/test distribution
    * shift Task 4B found): actual=1 rows correctly predicted as 1, divided by all actual=1 rows. */
  def positiveClassRecall(metrics: Metrics): Double = {
    val truePositives = metrics.confusionMatrix(1)(1)
    val actualPositives = metrics.confusionMatrix(1)(0) + metrics.confusionMatrix(1)(1)
    if (actualPositives > 0) truePositives.toDouble / actualPositives else Double.NaN
  }

  /** Extracts (featureName, importance) pairs from the fitted RandomForest stage, sorted by
    * importance descending. "Model feature importance," not causal significance -- Task 10's
    * explicit wording distinction. */
  def topFeatureImportances(model: PipelineModel, featureColumns: Seq[String], topN: Int): Seq[(String, Double)] = {
    val rfModel = model.stages.collectFirst { case m: RandomForestClassificationModel => m }
      .getOrElse(throw new IllegalStateException("no RandomForestClassificationModel stage found in the fitted pipeline"))
    val importances = rfModel.featureImportances
    featureColumns.zipWithIndex
      .map { case (name, idx) => name -> importances(idx) }
      .sortBy(-_._2)
      .take(topN)
  }
}
