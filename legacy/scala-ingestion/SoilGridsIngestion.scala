package smartagri

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.RasterExtent
import geotrellis.proj4.{LatLng, Transform}
import scala.collection.mutable

/**
 * SoilGrids ingestion (Scala + GeoTrellis).
 * Reads 7 SoilGrids GeoTIFFs from HDFS, samples each raster at the NDVI grid
 * cells, converts units, nulls out-of-coverage zeros, writes Parquet to HDFS.
 *
 * All raster decoding + sampling runs on the driver (plain Scala loops) so
 * GeoTrellis objects are never serialized across Spark tasks. Only the final
 * table is turned into a DataFrame and written.
 */
object SoilGridsIngestion {
  import HdfsUtils.HDFS

  val RAW  = s"$HDFS/user/smart_agri/raw/soilgrids"
  val GRID = s"$HDFS/user/smart_agri/processed/sentinel2_ndvi.parquet"
  val OUT  = s"$HDFS/user/smart_agri/processed/soilgrids_scala.parquet"

  private val rules = Seq(
    "ph"      -> ("ph_water", 10.0),
    "nir"     -> ("nitrogen_g_kg", 100.0),
    "organic" -> ("soil_organic_carbon", 10.0),
    "clay"    -> ("clay_pct", 10.0),
    "sand"    -> ("sand_pct", 10.0),
    "slit"    -> ("silt_pct", 10.0),
    "silt"    -> ("silt_pct", 10.0),
    "wat"     -> ("water_content_fc", 10.0)
  )

  private def classify(name: String): (String, Double) = {
    val low = name.toLowerCase
    rules.find { case (k, _) => low.contains(k) }.map(_._2)
      .getOrElse((name.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase, 1.0))
  }

  private def sample(tiff: SinglebandGeoTiff, lon: Double, lat: Double): Double = {
    val transform = Transform(LatLng, tiff.crs)
    val (x, y) = transform(lon, lat)
    if (!tiff.extent.contains(x, y)) return Double.NaN
    val re = RasterExtent(tiff.extent, tiff.tile.cols, tiff.tile.rows)
    val (c, r) = re.mapToGrid(x, y)
    tiff.tile.getDouble(c, r)
  }

  def run(spark: SparkSession): Unit = {
    val fs = HdfsUtils.hdfs(spark.sparkContext.hadoopConfiguration)

    // Pull the grid to the driver as plain arrays (no Spark ops after this).
    val gridArr = spark.read.parquet(GRID)
      .select("tile", "cell_row", "cell_col", "lat", "lon").distinct().collect()
    val n = gridArr.length
    println(s"Grid has $n cells")

    val tile = new Array[String](n)
    val cr   = new Array[Int](n)
    val cc   = new Array[Int](n)
    val lat  = new Array[Double](n)
    val lon  = new Array[Double](n)
    var i = 0
    while (i < n) {
      val row = gridArr(i)
      tile(i) = row.getString(0)
      cr(i)   = row.getAs[Number](1).intValue
      cc(i)   = row.getAs[Number](2).intValue
      lat(i)  = row.getAs[Number](3).doubleValue
      lon(i)  = row.getAs[Number](4).doubleValue
      i += 1
    }

    // For each cell, collect one value per soil column (all on the driver).
    val tifs = fs.listStatus(new Path(RAW)).filter(_.getPath.getName.endsWith(".tif"))
    val soilCols = tifs.map(st => classify(st.getPath.getName)._1).distinct.toSeq
    val values: Array[mutable.Map[String, java.lang.Double]] =
      Array.fill(n)(mutable.Map[String, java.lang.Double]())

    for (st <- tifs) {
      val name = st.getPath.getName
      val (colName, factor) = classify(name)
      println(s"  $name -> $colName (/$factor)")
      val tiff = SinglebandGeoTiff(HdfsUtils.readBytes(fs, st.getPath))
      var k = 0
      while (k < n) {
        val v = sample(tiff, lon(k), lat(k))
        if (!v.isNaN && v != 0.0) values(k).put(colName, java.lang.Double.valueOf(v / factor))
        k += 1
      }
    }

    // Build rows (nulls where a value is missing) and write Parquet.
    val rows = new Array[Row](n)
    var j = 0
    while (j < n) {
      val base = Seq[Any](tile(j), cr(j), cc(j), lat(j), lon(j))
      val soil = soilCols.map(c => values(j).getOrElse(c, null))
      rows(j) = Row.fromSeq(base ++ soil)
      j += 1
    }

    val schema = StructType(
      Seq(
        StructField("tile", StringType),
        StructField("cell_row", IntegerType),
        StructField("cell_col", IntegerType),
        StructField("lat", DoubleType),
        StructField("lon", DoubleType)
      ) ++ soilCols.map(c => StructField(c, DoubleType))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.write.mode("overwrite").parquet(OUT)
    println(s"Wrote ${df.count()} rows -> $OUT")
    df.describe(soilCols: _*).show()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SoilGridsIngestion").getOrCreate()
    run(spark)
    spark.stop()
  }
}