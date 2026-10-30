package smartagri.preprocess

/**
 * WGS84 UTM -> geographic (lat/lon) inverse projection (Snyder, USGS PP 1395,
 * eqs. 8-12..8-25). Sub-metre accurate inside a UTM zone -- far below the
 * ~1.1 km grid-cell size it is used for -- and dependency-free, so it runs
 * inside Spark executors without shipping a projection library.
 */
object UtmProjection {
  private val a = 6378137.0
  private val f = 1.0 / 298.257223563
  private val k0 = 0.9996
  private val e2 = f * (2 - f)
  private val ep2 = e2 / (1 - e2)
  private val e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))

  /** Parses "EPSG:326zz" (north) / "EPSG:327zz" (south) into (zone, isNorth). */
  def zoneFromEpsg(epsg: String): (Int, Boolean) = {
    val code = epsg.replaceAll("[^0-9]", "").toInt
    if (code >= 32601 && code <= 32660) (code - 32600, true)
    else if (code >= 32701 && code <= 32760) (code - 32700, false)
    else throw new IllegalArgumentException(s"not a WGS84 UTM EPSG code: $epsg")
  }

  /** Returns (lat, lon) in degrees. */
  def toLatLon(easting: Double, northing: Double, zone: Int, north: Boolean): (Double, Double) = {
    val x = easting - 500000.0
    val y = if (north) northing else northing - 10000000.0
    val lon0 = math.toRadians((zone - 1) * 6 - 180 + 3)

    val m = y / k0
    val mu = m / (a * (1 - e2 / 4 - 3 * e2 * e2 / 64 - 5 * e2 * e2 * e2 / 256))
    val phi1 = mu +
      (3 * e1 / 2 - 27 * math.pow(e1, 3) / 32) * math.sin(2 * mu) +
      (21 * e1 * e1 / 16 - 55 * math.pow(e1, 4) / 32) * math.sin(4 * mu) +
      (151 * math.pow(e1, 3) / 96) * math.sin(6 * mu) +
      (1097 * math.pow(e1, 4) / 512) * math.sin(8 * mu)

    val sin1 = math.sin(phi1); val cos1 = math.cos(phi1); val tan1 = math.tan(phi1)
    val c1 = ep2 * cos1 * cos1
    val t1 = tan1 * tan1
    val n1 = a / math.sqrt(1 - e2 * sin1 * sin1)
    val r1 = a * (1 - e2) / math.pow(1 - e2 * sin1 * sin1, 1.5)
    val d = x / (n1 * k0)

    val lat = phi1 - (n1 * tan1 / r1) * (
      d * d / 2 -
        (5 + 3 * t1 + 10 * c1 - 4 * c1 * c1 - 9 * ep2) * math.pow(d, 4) / 24 +
        (61 + 90 * t1 + 298 * c1 + 45 * t1 * t1 - 252 * ep2 - 3 * c1 * c1) * math.pow(d, 6) / 720)
    val lon = lon0 + (d -
      (1 + 2 * t1 + c1) * math.pow(d, 3) / 6 +
      (5 - 2 * c1 + 28 * t1 - 3 * c1 * c1 + 8 * ep2 + 24 * t1 * t1) * math.pow(d, 5) / 120) / cos1

    (math.toDegrees(lat), math.toDegrees(lon))
  }
}
