package smartagri

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
 * Smart Agriculture Analytics — Scala/Spark.
 *
 * Stage 1  : Join NDVI + SoilGrids into one analytics table.
 * Stage 2  : Novelty 1+2 — Reliability-aware Kalman filter via flatMapGroups:
 *            fuses sparse satellite NDVI with IoT soil proxy, propagates a
 *            continuous daily crop-vigour state with uncertainty.
 * Stage 3  : Derive crop-stress label from Kalman state.
 * Stage 4  : Novelty 3 — MLlib Random Forest predicts stress from soil features.
 * Stage 5  : Irrigation recommendation from soil-water balance + uncertainty.
 */
object Analytics {
  val HDFS    = HdfsUtils.HDFS
  val PROC    = s"$HDFS/user/smart_agri/processed"
  val RESULTS = s"$HDFS/user/smart_agri/results"

  // Kalman parameters
  val ALPHA   = 0.05; val Q = 0.0008
  val R_SAT   = 0.0025; val R_IOT = 0.02; val IOT_STALE = 0.01

  case class KS(x: Double, P: Double, stale: Int)
  case class FI(tile:String, cr:Int, cc:Int, date:String,
                ndvi:Option[Double], swvl:Option[Double])
  case class FO(tile:String, cr:Int, cc:Int, date:String,
                vigour:Double, vvar:Double, ndvi_obs:Option[Double])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SmartAgriAnalytics")
      .config("spark.sql.shuffle.partitions","50").getOrCreate()
    import spark.implicits._

    // ── Stage 1: Join NDVI + Soil ──────────────────────────────────────────
    println("=== Stage 1: Join NDVI + SoilGrids ===")
    val ndvi = spark.read.parquet(s"$PROC/sentinel2_ndvi.parquet")
    val soil = spark.read.parquet(s"$PROC/soilgrids_scala.parquet")
    val joined = ndvi.join(soil.drop("lat","lon"),
                           Seq("tile","cell_row","cell_col"), "left")
    println(s"Integrated rows: ${joined.count()}")
    joined.write.mode("overwrite").parquet(s"$RESULTS/integrated.parquet")

    // ── Stage 2: Reliability-aware Kalman filter ───────────────────────────
    println("=== Stage 2: Novelty 1+2 — Kalman filter (flatMapGroups) ===")
    val ds = joined.select(
        col("tile"), col("cell_row").cast("int").as("cr"),
        col("cell_col").cast("int").as("cc"), col("date"),
        col("ndvi").cast("double").as("ndvi"),
        col("water_content_fc").cast("double").as("swvl"))
      .as[FI].orderBy("tile","cr","cc","date")

    val kalman = ds.groupByKey(r => (r.tile, r.cr, r.cc))
      .flatMapGroups { case ((tile,cr,cc), iter) =>
        var s = KS(0.4, 0.05, 0)
        iter.map { row =>
          // Predict
          val xP = s.x + ALPHA * (0.4 - s.x); val PP = s.P + Q
          // Reliability-aware fusion (Novelty 1)
          val zs = scala.collection.mutable.ArrayBuffer[(Double,Double)]()
          row.ndvi.foreach(z => zs += ((z, R_SAT)))
          row.swvl.foreach(v => zs += ((math.min(0.9,0.15+v/100.0),
                                        R_IOT + IOT_STALE*s.stale)))
          val stale = if (row.swvl.isDefined) 0 else s.stale+1
          // Update (Novelty 2)
          var xN=xP; var PN=PP
          if (zs.nonEmpty) {
            val tp=zs.map(zr=>1.0/zr._2).sum
            val zF=zs.map{case(z,r)=>z/r}.sum/tp
            val K=PP/(PP+1.0/tp); xN=xP+K*(zF-xP); PN=(1-K)*PP
          }
          xN=math.max(0.0,math.min(1.0,xN))
          s=KS(xN,PN,stale)
          FO(tile,cr,cc,row.date,xN,PN,row.ndvi)
        }
      }
    kalman.write.mode("overwrite").parquet(s"$RESULTS/kalman_states.parquet")
    val kCount = spark.read.parquet(s"$RESULTS/kalman_states.parquet").count()
    println(s"Kalman states written: $kCount field-days")

    // ── Stage 3: Stress label ──────────────────────────────────────────────
    println("=== Stage 3: Crop-stress label ===")
    val states = spark.read.parquet(s"$RESULTS/kalman_states.parquet")
    val withM  = states.withColumn("month", month(to_date(col("date"))))
    val meds   = withM.groupBy("month")
      .agg(expr("percentile_approx(vigour,0.5)").as("med"))
    val labeled = withM.join(meds,"month")
      .withColumn("stressed",
        when(col("vigour") < col("med")-0.10, 1.0).otherwise(0.0)).drop("med")
    labeled.write.mode("overwrite").parquet(s"$RESULTS/labeled.parquet")
    val prev = labeled.filter(col("stressed")===1.0).count().toDouble/labeled.count()
    println(f"Stress prevalence: $prev%.2f")

    // ── Stage 4: MLlib Random Forest ──────────────────────────────────────
    println("=== Stage 4: Novelty 3 — MLlib Random Forest ===")
    val full = labeled.join(
      soil.select("tile","cell_row","cell_col","ph_water","nitrogen_g_kg",
        "soil_organic_carbon","clay_pct","sand_pct","silt_pct","water_content_fc"),
      Seq("tile","cell_row","cell_col"),"left")

    val featCols = Array("ph_water","nitrogen_g_kg","soil_organic_carbon",
      "clay_pct","sand_pct","silt_pct","water_content_fc","vvar")
    val asm = new VectorAssembler().setInputCols(featCols)
      .setOutputCol("features").setHandleInvalid("skip")
    val data = asm.transform(full.na.drop())
      .select(col("features"),col("stressed").as("label"))

    val Array(tr,te) = data.randomSplit(Array(0.7,0.3),seed=42)
    val rf = new RandomForestClassifier().setNumTrees(120).setMaxDepth(10)
      .setLabelCol("label").setFeaturesCol("features").setSeed(42)
    val model = rf.fit(tr)
    val pred  = model.transform(te)

    val ev = new MulticlassClassificationEvaluator()
      .setLabelCol("label").setPredictionCol("prediction")
    val acc = ev.setMetricName("accuracy").evaluate(pred)
    val f1  = ev.setMetricName("f1").evaluate(pred)
    println(f"Random Forest  accuracy=$acc%.3f   F1=$f1%.3f")
    println("Top feature importances:")
    featCols.zip(model.featureImportances.toArray).sortBy(-_._2).take(6)
      .foreach{case(f,i)=>println(f"  $f%-30s $i%.4f")}

    // ── Stage 5: Irrigation ────────────────────────────────────────────────
    println("=== Stage 5: Irrigation recommendation ===")
    val irr = labeled.join(
      soil.select("tile","cell_row","cell_col","water_content_fc"),
      Seq("tile","cell_row","cell_col"),"left")
      .withColumn("deficit", greatest(lit(0.0),
        col("water_content_fc")/100.0 - col("vigour")*0.3))
      .withColumn("irrigation_mm",
        round(col("deficit")*100.0*(lit(1.0)+lit(0.3)*col("vvar")*100.0),1))
    irr.select("tile","cr","cc","date","irrigation_mm","stressed")
      .write.mode("overwrite").parquet(s"$RESULTS/irrigation.parquet")
    val mi = irr.agg(avg("irrigation_mm")).first().getDouble(0)
    println(f"Mean irrigation: $mi%.1f mm")

    println("=== All stages complete — results in $RESULTS ===")
    spark.stop()
  }
}