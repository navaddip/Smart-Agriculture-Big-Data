package smartagri.ml

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import smartagri.features.FeaturePipelineBuilder

class CropStressModelSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("CropStressModelSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  test("the approved feature list (reused unmodified from Task 4B) contains no target/future/evaluation/leakage fields") {
    val featureColumns = FeaturePipelineBuilder.approvedFeatureColumns()
    val forbidden = Set(
      "future_anomaly", "future_stressed", "future_ndvi", "future_date",
      "corruptionType", "corruptionSeverity", "trueValue", "sensorHealth", "vvar"
    )
    forbidden.foreach(f => assert(!featureColumns.contains(f), s"feature list must not contain '$f'"))
    assert(!featureColumns.contains(CropStressModel.LabelColumn))
  }

  private val fixtureSchema = StructType(
    Seq(
      StructField("tile", StringType, false), StructField("cell_row", IntegerType, false),
      StructField("cell_col", IntegerType, false), StructField("date", StringType, false)
    ) ++ FeaturePipelineBuilder.approvedFeatureColumns().map(c => StructField(c, DoubleType, nullable = true)) ++
      Seq(StructField("future_stressed", IntegerType, nullable = false))
  )

  private def fixtureRow(tile: String, cellRow: Int, cellCol: Int, date: String, label: Int): Row = {
    val featureVals: Seq[Any] = FeaturePipelineBuilder.approvedFeatureColumns().map(_ => 0.3: Any)
    Row.fromSeq(Seq[Any](tile, cellRow, cellCol, date) ++ featureVals ++ Seq(label))
  }

  private def fixtureDf(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, fixtureSchema)
  }

  test("future_stressed only ever contains 0 or 1 in a valid fixture") {
    val df = fixtureDf(Seq(fixtureRow("T1", 0, 0, "2024-01-01", 0), fixtureRow("T1", 0, 1, "2024-01-01", 1)))
    val distinctLabels = df.select("future_stressed").distinct().collect().map(_.getInt(0)).toSet
    assert(distinctLabels.subsetOf(Set(0, 1)))
  }

  test("sample_id is a stable, deterministic identity built from tile/cell_row/cell_col/date") {
    val df = MlDataPreparation.withSampleId(fixtureDf(Seq(fixtureRow("T1", 5, 12, "2024-04-01", 1))))
    val row = df.collect()(0)
    assert(row.getAs[String]("sample_id") == "T1|5|12|2024-04-01")
  }

  test("class weights are computed and both classes receive a positive weight") {
    val df = fixtureDf(Seq(
      fixtureRow("T1", 0, 0, "2024-01-01", 1),
      fixtureRow("T1", 0, 1, "2024-01-01", 0),
      fixtureRow("T1", 0, 2, "2024-01-01", 0)
    ))
    val balance = MlDataPreparation.classBalance(df)
    assert(balance.positiveCount == 1 && balance.negativeCount == 2)
    val weighted = MlDataPreparation.withClassWeights(df)
    val weights = weighted.select("class_weight").collect().map(_.getDouble(0))
    assert(weights.forall(_ > 0.0))
    val posWeight = weighted.filter("future_stressed = 1").select("class_weight").collect()(0).getDouble(0)
    val negWeight = weighted.filter("future_stressed = 0").select("class_weight").collect()(0).getDouble(0)
    assert(posWeight > negWeight, "the minority (positive) class must receive a larger weight")
  }

  test("the pipeline fits on a small fixture and produces predictions with the expected columns") {
    val rows = (0 until 20).map(i => fixtureRow("T1", 0, i, "2024-01-01", i % 3 match { case 0 => 1; case _ => 0 }))
    val df = MlDataPreparation.withClassWeights(fixtureDf(rows))
    val config = MlConfig.default().copy(numTrees = 5, maxDepth = 3)
    val model = CropStressModel.fit(df, FeaturePipelineBuilder.approvedFeatureColumns(), config)
    val predictions = CropStressModel.predict(model, df)
    Seq("prediction", "probability", "rawPrediction").foreach { c =>
      assert(predictions.columns.contains(c), s"predictions must contain '$c'")
    }
  }

  test("evaluate() computes all required metrics without throwing, on a small fitted-model fixture") {
    val rows = (0 until 20).map(i => fixtureRow("T1", 0, i, "2024-01-01", i % 3 match { case 0 => 1; case _ => 0 }))
    val df = MlDataPreparation.withClassWeights(fixtureDf(rows))
    val config = MlConfig.default().copy(numTrees = 5, maxDepth = 3)
    val model = CropStressModel.fit(df, FeaturePipelineBuilder.approvedFeatureColumns(), config)
    val predictions = CropStressModel.predict(model, df)
    val metrics = ModelEvaluation.evaluate(predictions)
    assert(metrics.accuracy >= 0.0 && metrics.accuracy <= 1.0)
    assert(metrics.confusionMatrix.length == 2 && metrics.confusionMatrix(0).length == 2)
  }

  test("training twice with the same seed on the same data produces identical predictions") {
    val rows = (0 until 20).map(i => fixtureRow("T1", 0, i, "2024-01-01", i % 3 match { case 0 => 1; case _ => 0 }))
    val df = MlDataPreparation.withClassWeights(fixtureDf(rows))
    val config = MlConfig.default().copy(numTrees = 5, maxDepth = 3)

    val model1 = CropStressModel.fit(df, FeaturePipelineBuilder.approvedFeatureColumns(), config)
    val model2 = CropStressModel.fit(df, FeaturePipelineBuilder.approvedFeatureColumns(), config)

    val preds1 = CropStressModel.predict(model1, df).select("prediction").collect().map(_.getDouble(0))
    val preds2 = CropStressModel.predict(model2, df).select("prediction").collect().map(_.getDouble(0))
    assert(preds1.toSeq == preds2.toSeq, "the same seed on the same data must produce identical predictions")
  }
}
