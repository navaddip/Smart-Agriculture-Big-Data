package smartagri.canonical

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Local-SparkSession tests for `SentinelDeduplication` (Task 3B section
 * K items 1-2). Uses `master("local[2]")` -- a standard, legitimate way
 * to unit-test Spark logic, not a cluster integration test.
 */
class SentinelDeduplicationSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("SentinelDeduplicationSpec").master("local[2]")
      .config("spark.ui.enabled", "false").getOrCreate()
  }
  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val schema = StructType(Seq(
    StructField("tile", StringType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("cell_row", IntegerType, nullable = false),
    StructField("cell_col", IntegerType, nullable = false),
    StructField("lat", DoubleType, nullable = false),
    StructField("lon", DoubleType, nullable = false),
    StructField("ndvi", DoubleType, nullable = false)
  ))

  private def df(rows: Seq[Row]) = {
    import scala.collection.JavaConverters._
    spark.createDataFrame(rows.asJava, schema)
  }

  // -- 1. Sentinel exact-key deduplication ------------------------------

  test("an exact byte-identical duplicate is collapsed to a single row") {
    val source = df(Seq(
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099), // exact duplicate
      Row("T43PGQ", "2023-04-23", 0, 7, 13.556076, 76.923936, 0.313574)  // unique row
    ))
    val (deduped, result) = SentinelDeduplication.deduplicate(source)

    assert(result.originalRows == 3)
    assert(result.duplicateKeyCount == 1)
    assert(result.duplicateRowsRemoved == 1)
    assert(result.finalRows == 2)
    assert(deduped.count() == 2)
  }

  test("a table with no duplicates at all is returned unchanged") {
    val source = df(Seq(
      Row("T43PHQ", "2023-04-13", 0, 0, 13.5, 77.5, 0.4),
      Row("T43PHQ", "2023-04-13", 0, 1, 13.5, 77.6, 0.5)
    ))
    val (deduped, result) = SentinelDeduplication.deduplicate(source)
    assert(result.duplicateKeyCount == 0)
    assert(result.duplicateRowsRemoved == 0)
    assert(deduped.count() == 2)
  }

  test("deduplication produces zero remaining duplicate keys") {
    val source = df(Seq(
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099) // three identical copies
    ))
    val (deduped, _) = SentinelDeduplication.deduplicate(source)
    val remainingDupes = deduped.groupBy("tile", "date", "cell_row", "cell_col").count().filter("count > 1").count()
    assert(remainingDupes == 0)
    assert(deduped.count() == 1)
  }

  // -- 2. Conflicting duplicate detection --------------------------------

  test("a duplicate key with CONFLICTING ndvi values aborts with IllegalStateException, never silently resolved") {
    val source = df(Seq(
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.999999) // same key, DIFFERENT ndvi
    ))
    val ex = intercept[IllegalStateException] {
      SentinelDeduplication.deduplicate(source)
    }
    assert(ex.getMessage.contains("CONFLICTING"))
  }

  test("a duplicate key with conflicting lat/lon (but same ndvi) also aborts") {
    val source = df(Seq(
      Row("T43PGQ", "2023-04-23", 0, 6, 13.556154, 76.913793, 0.341099),
      Row("T43PGQ", "2023-04-23", 0, 6, 13.999999, 76.913793, 0.341099) // same key, different lat
    ))
    assertThrows[IllegalStateException] {
      SentinelDeduplication.deduplicate(source)
    }
  }

  test("verifyDuplicatesAreIdentical does not throw when there are no conflicts (including no duplicates at all)") {
    val source = df(Seq(Row("T43PHQ", "2023-04-13", 0, 0, 13.5, 77.5, 0.4)))
    SentinelDeduplication.verifyDuplicatesAreIdentical(source) // must not throw
  }
}
