import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.io.ByteArrayOutputStream

import geotrellis.raster.io.geotiff.SinglebandGeoTiff

object SoilGridsPreprocess {

  case class RasterData(
      name: String,
      path: String,
      scale: Double,
      values: Array[Array[Int]],
      cols: Int,
      rows: Int,
      xmin: Double,
      ymin: Double,
      xmax: Double,
      ymax: Double,
      crs: String
  )

  def readHdfsBytes(fs: FileSystem, path: String): Array[Byte] = {
    val in = fs.open(new Path(path))
    val out = new ByteArrayOutputStream()
    try {
      val buffer = new Array[Byte](1024 * 1024)
      var n = in.read(buffer)
      while (n > 0) {
        out.write(buffer, 0, n)
        n = in.read(buffer)
      }
      out.toByteArray
    } finally {
      in.close()
      out.close()
    }
  }

  def loadRaster(
      fs: FileSystem,
      fileName: String,
      outputName: String,
      scale: Double
  ): RasterData = {

    val path =
      s"hdfs://namenode:9000/user/smart_agri/raw/soilgrids/$fileName"

    println(s"\nReading $fileName")
    println(s"HDFS path: $path")

    val bytes = readHdfsBytes(fs, path)
    val geotiff = SinglebandGeoTiff(bytes)
    val raster = geotiff.raster
    val tile = raster.tile
    val extent = raster.extent

    println(s"  CRS      : ${geotiff.crs}")
    println(s"  Extent   : $extent")
    println(s"  Size     : ${tile.cols} x ${tile.rows}")
    println(s"  CellType : ${tile.cellType}")
    println(s"  Scale    : $scale")

    val values = Array.ofDim[Int](tile.rows, tile.cols)

    var r = 0
    while (r < tile.rows) {
      var c = 0
      while (c < tile.cols) {
        values(r)(c) = tile.get(c, r)
        c += 1
      }
      r += 1
    }

    RasterData(
      outputName,
      path,
      scale,
      values,
      tile.cols,
      tile.rows,
      extent.xmin,
      extent.ymin,
      extent.xmax,
      extent.ymax,
      geotiff.crs.toString
    )
  }

  /*
   * SoilGrids rasters used by this project are expected to be geographic
   * rasters whose coordinates correspond to longitude/latitude.
   *
   * The raster is sampled using nearest-neighbour lookup.
   *
   * If the CRS printed above is not geographic WGS84-compatible, stop and
   * adjust this function to reproject the NDVI coordinates before sampling.
   */
  def sampleNearest(r: RasterData, lon: Double, lat: Double): Option[Double] = {

    if (
      lon < r.xmin || lon > r.xmax ||
      lat < r.ymin || lat > r.ymax
    ) {
      return None
    }

    val colFraction =
      (lon - r.xmin) / (r.xmax - r.xmin) * r.cols

    val rowFraction =
      (r.ymax - lat) / (r.ymax - r.ymin) * r.rows

    var col = math.floor(colFraction).toInt
    var row = math.floor(rowFraction).toInt

    if (col == r.cols) col = r.cols - 1
    if (row == r.rows) row = r.rows - 1

    if (
      col < 0 || col >= r.cols ||
      row < 0 || row >= r.rows
    ) {
      None
    } else {
      val raw = r.values(row)(col)

      // SoilGrids raw 0 means outside soil coverage.
      if (raw == 0) None
      else Some(raw.toDouble / r.scale)
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SmartAgriculture-SoilGridsPreprocessing")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val hdfsUri = new URI("hdfs://namenode:9000")
    val fs = FileSystem.get(
      hdfsUri,
      spark.sparkContext.hadoopConfiguration
    )

    val ndviPath =
      "hdfs://namenode:9000/user/smart_agri/processed/sentinel2_ndvi.parquet"

    val outputPath =
      "hdfs://namenode:9000/user/smart_agri/processed/soilgrids.parquet"

    println("\n=== SMART AGRICULTURE: SOILGRIDS PREPROCESSING ===")
    println(s"NDVI input : $ndviPath")
    println(s"Output     : $outputPath")

    // ------------------------------------------------------------------
    // 1. Read the Scala-produced/validated NDVI grid.
    // ------------------------------------------------------------------

    val ndvi = spark.read.parquet(ndviPath)

    println("\nNDVI schema:")
    ndvi.printSchema()

    val requiredColumns =
      Set("tile", "cell_row", "cell_col", "lat", "lon")

    val missing =
      requiredColumns.diff(ndvi.columns.toSet)

    require(
      missing.isEmpty,
      s"NDVI parquet is missing columns: ${missing.mkString(", ")}"
    )

    val grid = ndvi
      .select(
        col("tile"),
        col("cell_row"),
        col("cell_col"),
        col("lat"),
        col("lon")
      )
      .dropDuplicates("tile", "cell_row", "cell_col")

    println(s"Unique NDVI grid cells: ${grid.count()}")

    // ------------------------------------------------------------------
    // 2. Load all seven SoilGrids rasters from HDFS.
    // ------------------------------------------------------------------

    val ph = loadRaster(
      fs,
      "PH_WATER.tif",
      "ph_water",
      10.0
    )

    val nitrogen = loadRaster(
      fs,
      "Nirtogen.tif",
      "nitrogen_g_kg",
      100.0
    )

    val organicCarbon = loadRaster(
      fs,
      "Solid organic carbon.tif",
      "soil_organic_carbon",
      10.0
    )

    val clay = loadRaster(
      fs,
      "clay content.tif",
      "clay_pct",
      10.0
    )

    val sand = loadRaster(
      fs,
      "sand.tif",
      "sand_pct",
      10.0
    )

    val silt = loadRaster(
      fs,
      "slit.tif",
      "silt_pct",
      10.0
    )

    val waterContent = loadRaster(
      fs,
      "watevol content -33.tif",
      "water_content_fc",
      10.0
    )

    val rasters = Seq(
      ph,
      nitrogen,
      organicCarbon,
      clay,
      sand,
      silt,
      waterContent
    )

    // ------------------------------------------------------------------
    // 3. Collect only the small 100x100 NDVI grid to the driver.
    //
    // The target grid is ~20k cells, so this is intentionally small.
    // The large raw rasters remain in HDFS and are decoded one time.
    // ------------------------------------------------------------------

    val cells = grid.collect()

    println(s"\nSampling ${cells.length} grid cells...")

    val outputRows = cells.map { row =>

      val tile = row.getAs[String]("tile")
      val cellRow = row.getAs[Int]("cell_row")
      val cellCol = row.getAs[Int]("cell_col")
      val lat = row.getAs[Double]("lat")
      val lon = row.getAs[Double]("lon")

      val values =
        rasters.map(r => sampleNearest(r, lon, lat))

      (
        tile,
        cellRow,
        cellCol,
        lat,
        lon,
        values(0),
        values(1),
        values(2),
        values(3),
        values(4),
        values(5),
        values(6)
      )
    }

    // ------------------------------------------------------------------
    // 4. Convert sampled tuples into a Spark DataFrame.
    // ------------------------------------------------------------------

    import spark.implicits._

    val soilDF = outputRows.toSeq.toDF(
      "tile",
      "cell_row",
      "cell_col",
      "lat",
      "lon",
      "ph_water",
      "nitrogen_g_kg",
      "soil_organic_carbon",
      "clay_pct",
      "sand_pct",
      "silt_pct",
      "water_content_fc"
    )

    // ------------------------------------------------------------------
    // 5. Basic validation.
    // ------------------------------------------------------------------

    println("\nSoilGrid output schema:")
    soilDF.printSchema()

    println("\nFirst rows:")
    soilDF.show(10, truncate = false)

    println(s"Output rows: ${soilDF.count()}")

    println("\nNull counts:")
    soilDF.select(
      count(lit(1)).alias("total"),
      count("ph_water").alias("ph_non_null"),
      count("nitrogen_g_kg").alias("nitrogen_non_null"),
      count("soil_organic_carbon").alias("organic_carbon_non_null"),
      count("clay_pct").alias("clay_non_null"),
      count("sand_pct").alias("sand_non_null"),
      count("silt_pct").alias("silt_non_null"),
      count("water_content_fc").alias("water_content_non_null")
    ).show(false)

    // ------------------------------------------------------------------
    // 6. Write processed SoilGrids data back to HDFS as Parquet.
    // ------------------------------------------------------------------

    soilDF
      .repartition(2)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    println("\n=== SOILGRIDS PREPROCESSING COMPLETE ===")
    println(s"Written to: $outputPath")

    // Confirm the Parquet can be read back.
    val verify = spark.read.parquet(outputPath)

    println(s"Verified rows: ${verify.count()}")
    verify.printSchema()

    spark.stop()
  }
}
