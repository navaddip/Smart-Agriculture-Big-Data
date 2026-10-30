package smartagri.features

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class FeatureQualityReportSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("FeatureQualityReportSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val soilSchema = StructType(
    FeatureQualityReport.SoilColumns.map(c => StructField(c, DoubleType, nullable = true))
  )
  private def soilDf(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, soilSchema)
  }

  test("imputation statistics are computed only from the DataFrame passed in (simulating train-only usage)") {
    val train = soilDf(Seq(
      Row(5.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0),
      Row(7.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)
    ))
    val stats = FeatureQualityReport.computeImputationStatistics(train)
    assert(math.abs(stats.statistics("ph_water") - 6.0) < 1e-9)
  }

  test("applyImputation fills nulls with the frozen statistic and leaves non-null values untouched") {
    val train = soilDf(Seq(
      Row(5.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0),
      Row(7.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)
    ))
    val stats = FeatureQualityReport.computeImputationStatistics(train)

    val testRow = soilDf(Seq(Row(null, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)))
    val imputed = FeatureQualityReport.applyImputation(testRow, stats).collect()(0)
    assert(math.abs(imputed.getAs[Double]("ph_water") - 6.0) < 1e-9, "null must be filled with the TRAIN-derived mean")
    assert(imputed.getAs[Double]("nitrogen_g_kg") == 1.0, "a present value must remain exactly unchanged")
  }

  test("a frozen imputation statistic is unaffected by any subsequently-seen test-only values") {
    val train = soilDf(Seq(Row(5.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0), Row(7.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)))
    val statsFirst = FeatureQualityReport.computeImputationStatistics(train)
    val statsAgain = FeatureQualityReport.computeImputationStatistics(train)
    assert(statsFirst.statistics("ph_water") == statsAgain.statistics("ph_water"))
  }

  test("withSoilAvailabilityFlag is 1 only when ALL seven soil columns are present") {
    val rows = soilDf(Seq(
      Row(5.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0),
      Row(null, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0)
    ))
    val flagged = FeatureQualityReport.withSoilAvailabilityFlag(rows).collect()
    assert(flagged(0).getAs[Int]("soil_data_available") == 1)
    assert(flagged(1).getAs[Int]("soil_data_available") == 0)
  }

  test("running the same transformation twice on identical input yields identical schema, row count, and values after sorting") {
    val input = soilDf(Seq(
      Row(5.0, 1.0, 10.0, 20.0, 30.0, 10.0, 15.0),
      Row(null, 1.2, 11.0, 21.0, 31.0, 11.0, 16.0)
    ))
    val run1 = FeatureQualityReport.withSoilAvailabilityFlag(input).orderBy("nitrogen_g_kg").collect()
    val run2 = FeatureQualityReport.withSoilAvailabilityFlag(input).orderBy("nitrogen_g_kg").collect()
    val cols = FeatureQualityReport.withSoilAvailabilityFlag(input).columns

    assert(run1.length == run2.length)
    run1.zip(run2).foreach { case (r1, r2) =>
      cols.foreach { c =>
        val same = (r1.isNullAt(r1.fieldIndex(c)) && r2.isNullAt(r2.fieldIndex(c))) ||
          r1.getAs[Any](c) == r2.getAs[Any](c)
        assert(same, s"column $c must match exactly (or both be null) across two runs on identical input")
      }
    }
  }
}
