package smartagri.canonical

/**
 * Pure, Spark-independent geographic distance calculation, used by
 * `Era5NearestNeighbor` for ERA5-to-Sentinel spatial alignment (Task
 * 3B, section D).
 */
object GeoDistance {

  /** Mean Earth radius in kilometres (WGS84 mean radius), used throughout. */
  val EarthRadiusKm: Double = 6371.0088

  /**
   * Great-circle distance between two lat/lon points, in kilometres, via
   * the Haversine formula -- a correct geographic distance metric, never
   * an independent comparison of latitude and longitude deltas (Task 3B's
   * explicit requirement: "Do NOT compare latitude and longitude
   * independently").
   */
  def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) *
        math.sin(dLon / 2) * math.sin(dLon / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    EarthRadiusKm * c
  }
}
