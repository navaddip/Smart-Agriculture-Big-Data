// HdfsEvidence.scala — run in spark-shell to produce evidence for review
// Shows: raw datasets, processed datasets, sizes, sample rows from each
// Run: spark-shell --master local[*] (on the big_data_default network)
// Then :load /scripts/show_hdfs_evidence.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

val HDFS = "hdfs://namenode:9000"
val fs   = FileSystem.get(new URI(HDFS), spark.sparkContext.hadoopConfiguration)

def du(path: String): String = {
  try {
    val cs = fs.getContentSummary(new Path(path))
    val gb = cs.getLength / 1e9
    if (gb >= 1) f"$gb%.1f GB" else f"${cs.getLength / 1e6}%.1f MB"
  } catch { case _: Throwable => "N/A" }
}

println("\n" + "="*60)
println("EVIDENCE: HDFS Storage — Smart Agriculture Pipeline")
println("="*60)

println("\n--- RAW DATASETS (ingested from external sources) ---")
val rawPaths = Seq("sentinel2", "era5", "soilgrids")
rawPaths.foreach { name =>
  val p = s"$HDFS/user/smart_agri/raw/$name"
  val size = du(p)
  val count = try fs.listStatus(new Path(p)).length catch { case _: Throwable => 0 }
  println(f"  /raw/$name%-15s  $size%8s   ($count files)")
}
println(f"  TOTAL RAW: ${du(s"$HDFS/user/smart_agri/raw")}")

println("\n--- PROCESSED DATASETS (output of Scala preprocessing) ---")
val procPaths = Seq(
  ("soilgrids_scala.parquet",       "SoilGridsIngestion.scala (GeoTrellis GeoTIFF)"),
  ("era5_scala.parquet",            "Era5Ingestion.scala      (NetCDF-Java GRIB/NC)"),
  ("sentinel2_ndvi.parquet",        "sentinel2 NDVI           (NDVI computed)")
)
procPaths.foreach { case (name, source) =>
  val p = s"$HDFS/user/smart_agri/processed/$name"
  val size = du(p)
  println(f"  $name%-40s  $size%8s  <- $source")
}

println("\n--- SAMPLE DATA: SoilGrids (Scala output) ---")
val soil = spark.read.parquet(s"$HDFS/user/smart_agri/processed/soilgrids_scala.parquet")
println(s"  Total rows: ${soil.count()}")
println("  Schema: " + soil.columns.mkString(", "))
soil.show(3, truncate=false)

println("\n--- SAMPLE DATA: ERA5 Weather (Scala output) ---")
val era5 = spark.read.parquet(s"$HDFS/user/smart_agri/processed/era5_scala.parquet")
println(s"  Total rows: ${era5.count()}")
println("  Variables: " + era5.select("variable").distinct().collect().map(_.getString(0)).sorted.mkString(", "))
era5.show(5, truncate=false)

println("\n--- SAMPLE DATA: Sentinel-2 NDVI ---")
val ndvi = spark.read.parquet(s"$HDFS/user/smart_agri/processed/sentinel2_ndvi.parquet")
println(s"  Total rows: ${ndvi.count()}")
ndvi.describe("ndvi").show()

println("\n--- CLUSTER STATUS ---")
val nn = new Path(s"$HDFS/user/smart_agri")
println(s"  HDFS NameNode reachable: ${fs.exists(nn)}")
println(s"  Total project size: ${du(s"$HDFS/user/smart_agri")}")
println("="*60)
println("END OF EVIDENCE")
println("="*60 + "\n")