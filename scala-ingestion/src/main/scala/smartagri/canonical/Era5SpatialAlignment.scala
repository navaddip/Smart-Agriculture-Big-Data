package smartagri.canonical

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Attaches the full ERA5 weather vector to each Sentinel row, per Task
 * 3B section D: exact-date match, nearest-neighbour spatial match (no
 * temporal interpolation -- Sentinel dates are known, from Task 3A, to
 * have exact matching ERA5 dates).
 *
 * DELIBERATELY NOT a Cartesian join (Task 3B's explicit prohibition).
 * ERA5 has only 81 distinct spatial points -- small enough to collect
 * and broadcast to every executor (Task 3B section M's explicit
 * allowance) -- so the nearest-point search happens as a broadcasted-
 * list lookup inside one UDF call per Sentinel row, not as a Spark join
 * against ERA5's full 59,211 rows. The Sentinel side (hundreds of
 * thousands of rows) is never collected to the driver and remains fully
 * distributed throughout; only ERA5's small set of distinct (lat, lon)
 * points is collected.
 */
object Era5SpatialAlignment {

  final case class AlignmentResult(
      sentinelRowsBefore: Long,
      matchedRows: Long,
      unmatchedRows: Long,
      era5PointsUsed: Long,
      maxDistanceKm: Double,
      meanDistanceKm: Double,
      unmatchedDates: Seq[String]
  )

  def align(spark: SparkSession, sentinel: DataFrame, era5: DataFrame): (DataFrame, AlignmentResult) = {
    import spark.implicits._

    val sentinelRowsBefore = sentinel.count()

    // Small enough to collect (81 rows): the distinct ERA5 spatial points, broadcast once.
    val era5Points: Array[Era5Point] = era5.select("lat", "lon").distinct().as[Era5Point].collect()
    val era5PointsUsed = era5Points.length.toLong
    val broadcastPoints = spark.sparkContext.broadcast(era5Points.toSeq)

    // One UDF call per row computes the nearest point AND its distance together (a struct),
    // rather than three separate UDF calls each redoing the same nearest-neighbour search.
    val nearestUdf = udf { (lat: Double, lon: Double) =>
      val (point, distanceKm) = Era5NearestNeighbor.nearestWithDistance(lat, lon, broadcastPoints.value)
      (point.lat, point.lon, distanceKm)
    }

    val withNearest = sentinel
      .withColumn("_nearest", nearestUdf(col("lat"), col("lon")))
      .withColumn("era5_source_lat", col("_nearest._1"))
      .withColumn("era5_source_lon", col("_nearest._2"))
      .withColumn("era5_match_distance_km", col("_nearest._3"))
      .drop("_nearest")

    val era5ForJoin = era5.withColumnRenamed("lat", "era5_source_lat").withColumnRenamed("lon", "era5_source_lon")

    val joined = withNearest
      .join(
        era5ForJoin,
        withNearest("date") === era5ForJoin("date") &&
          withNearest("era5_source_lat") === era5ForJoin("era5_source_lat") &&
          withNearest("era5_source_lon") === era5ForJoin("era5_source_lon"),
        "left"
      )
      .drop(era5ForJoin("date"))
      .drop(era5ForJoin("era5_source_lat"))
      .drop(era5ForJoin("era5_source_lon"))

    joined.cache()

    val matchedRows = joined.filter(col("swvl1").isNotNull).count()
    val unmatchedRows = joined.count() - matchedRows

    val unmatchedDates: Seq[String] =
      if (unmatchedRows > 0)
        joined.filter(col("swvl1").isNull).select("date").distinct().as[String].collect().toSeq.sorted
      else Seq.empty

    val distanceStats = joined.agg(max("era5_match_distance_km"), avg("era5_match_distance_km")).head()
    val maxDistanceKm = distanceStats.getAs[Double](0)
    val meanDistanceKm = distanceStats.getAs[Double](1)

    val result = AlignmentResult(
      sentinelRowsBefore = sentinelRowsBefore,
      matchedRows = matchedRows,
      unmatchedRows = unmatchedRows,
      era5PointsUsed = era5PointsUsed,
      maxDistanceKm = maxDistanceKm,
      meanDistanceKm = meanDistanceKm,
      unmatchedDates = unmatchedDates
    )

    (joined, result)
  }
}
