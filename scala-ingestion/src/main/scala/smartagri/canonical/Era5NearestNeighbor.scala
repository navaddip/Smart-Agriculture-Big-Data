package smartagri.canonical

/** One ERA5 native grid point (lat/lon only -- the weather values themselves are attached
  * downstream by an exact-date join, not carried by this type). */
final case class Era5Point(lat: Double, lon: Double)

/**
 * Pure, Spark-independent nearest-neighbour selection over a small set
 * of ERA5 spatial points (Task 3B, section D).
 *
 * Isolated here specifically so the selection and tie-breaking logic can
 * be unit-tested with small, hand-computable fixtures, independent of
 * Spark or any UDF wiring. `Era5SpatialAlignment` wraps this in a UDF
 * against a broadcasted list of ERA5 points -- it contains no distance
 * or tie-break logic of its own.
 */
object Era5NearestNeighbor {

  /**
   * Finds the nearest candidate to `(targetLat, targetLon)` by Haversine
   * distance, returning it alongside that distance.
   *
   * DETERMINISTIC TIE-BREAK (Task 3B's explicit requirement): if two or
   * more candidates are at the exact same distance, the one with the
   * lowest latitude is chosen; if latitudes also tie, the lowest
   * longitude breaks the tie. This is achieved by sorting on the tuple
   * `(distance, lat, lon)`, which is a total order (no two candidates
   * with distinct lat/lon can tie on all three), so the result is always
   * well-defined and reproducible across runs.
   */
  def nearestWithDistance(
      targetLat: Double,
      targetLon: Double,
      candidates: Seq[Era5Point]
  ): (Era5Point, Double) = {
    require(candidates.nonEmpty, "candidates must not be empty")
    candidates
      .map(p => (p, GeoDistance.haversineKm(targetLat, targetLon, p.lat, p.lon)))
      .sortBy { case (p, d) => (d, p.lat, p.lon) }
      .head
  }

  /** Convenience form returning only the nearest point, when the distance itself isn't needed. */
  def nearest(targetLat: Double, targetLon: Double, candidates: Seq[Era5Point]): Era5Point =
    nearestWithDistance(targetLat, targetLon, candidates)._1
}
