package smartagri

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import ucar.nc2.{NetcdfFile, Variable}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 * ERA5 ingestion (Scala + NetCDF-Java / cdm 4.5.5 from Maven Central).
 *
 * NetcdfFile.open() reads BOTH GRIB and NetCDF, so this works on the existing
 * GRIB download (no re-download needed). It stages the file locally, discovers
 * the latitude / longitude axes and every (time, lat, lon) weather variable,
 * and writes a long-format table to HDFS as Parquet:
 *
 *     variable, time_index, lat, lon, value
 *
 * Daily aggregation / unit conversion are done downstream in Spark SQL.
 * All decoding runs on the driver (no Spark serialization of NetCDF objects).
 */
object Era5Ingestion {
  import HdfsUtils.HDFS

  val RAW = s"$HDFS/user/smart_agri/raw/era5"
  val OUT = s"$HDFS/user/smart_agri/processed/era5_scala.parquet"

  /** Stage the raw file locally: unzip a .grib/.nc out of a CDS zip, or copy it. */
  private def stageLocal(fs: FileSystem): String = {
    val entries = fs.listStatus(new Path(RAW)).map(_.getPath)
    val zipOpt = entries.find(_.getName.toLowerCase.endsWith(".zip"))
    zipOpt match {
      case Some(zip) =>
        val zin = new java.util.zip.ZipInputStream(fs.open(zip))
        var entry = zin.getNextEntry
        while (entry != null &&
               !(entry.getName.toLowerCase.matches(".*\\.(grib|grib2|nc)$") ||
                 entry.getName.toLowerCase.contains("grib"))) entry = zin.getNextEntry
        require(entry != null, "no .grib/.nc found inside the ERA5 zip")
        val suffix = if (entry.getName.toLowerCase.endsWith(".nc")) ".nc" else ".grib"
        val tmp = java.io.File.createTempFile("era5_", suffix); tmp.deleteOnExit()
        val out = new java.io.FileOutputStream(tmp)
        org.apache.commons.io.IOUtils.copy(zin, out); out.close(); zin.close()
        tmp.getAbsolutePath
      case None =>
        val f = entries.find(p => p.getName.toLowerCase.matches(".*\\.(grib|grib2|nc)$"))
          .getOrElse(sys.error("no .grib/.nc/.zip in raw/era5"))
        HdfsUtils.toLocalTemp(fs, f, "." + f.getName.split('.').last)
    }
  }

  /** Read a 1-D coordinate variable into a Double array. */
  private def read1D(v: Variable): Array[Double] = {
    val a = v.read(); val n = a.getSize.toInt
    val out = new Array[Double](n)
    var k = 0; while (k < n) { out(k) = a.getDouble(k); k += 1 }
    out
  }

  def run(spark: SparkSession): Unit = {
    val fs = HdfsUtils.hdfs(spark.sparkContext.hadoopConfiguration)
    val local = stageLocal(fs)
    println(s"Opening: $local")

    val nc = NetcdfFile.open(local)
    try {
      val allVars = nc.getVariables.asScala.toSeq
      println("Variables: " + allVars.map(_.getShortName).mkString(", "))

      def findAxis(keys: String*): Variable =
        allVars.find(v => v.getRank == 1 && keys.exists(k => v.getShortName.toLowerCase.contains(k)))
          .getOrElse(sys.error(s"no axis matching ${keys.mkString("/")}"))

      val lats = read1D(findAxis("latitude", "lat"))
      val lons = read1D(findAxis("longitude", "lon"))
      println(s"grid: ${lats.length} lats x ${lons.length} lons")

      val rows = new ArrayBuffer[Row]()
      val dataVars = allVars.filter(_.getRank == 3)   // (time, lat, lon)
      for (v <- dataVars) {
        val arr = v.read()
        val shape = v.getShape
        val nt = shape(0); val nla = shape(1); val nlo = shape(2)
        val idx = arr.getIndex
        var t = 0
        while (t < nt) {
          var i = 0
          while (i < nla) {
            var j = 0
            while (j < nlo) {
              val value = arr.getDouble(idx.set(t, i, j))
              rows += Row(v.getShortName, t, lats(i), lons(j), value)
              j += 1
            }
            i += 1
          }
          t += 1
        }
        println(s"  ${v.getShortName}: ${nt}x${nla}x${nlo}")
      }

      val schema = StructType(Seq(
        StructField("variable", StringType), StructField("time_index", IntegerType),
        StructField("lat", DoubleType), StructField("lon", DoubleType),
        StructField("value", DoubleType)))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      df.write.mode("overwrite").parquet(OUT)
      println(s"Wrote ${df.count()} rows -> $OUT")
    } finally nc.close()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Era5Ingestion").getOrCreate()
    run(spark)
    spark.stop()
  }
}