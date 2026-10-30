package smartagri.canonical

import org.scalatest.funsuite.AnyFunSuite

class GeoDistanceSpec extends AnyFunSuite {

  test("distance from a point to itself is zero") {
    val d = GeoDistance.haversineKm(13.0, 77.0, 13.0, 77.0)
    assert(math.abs(d) < 1e-9)
  }

  test("distance is symmetric") {
    val d1 = GeoDistance.haversineKm(13.0, 77.0, 13.5, 77.5)
    val d2 = GeoDistance.haversineKm(13.5, 77.5, 13.0, 77.0)
    assert(math.abs(d1 - d2) < 1e-9)
  }

  test("one degree of latitude is approximately 111 km") {
    val d = GeoDistance.haversineKm(13.0, 77.0, 14.0, 77.0)
    assert(math.abs(d - 111.0) < 2.0) // WGS84 mean radius gives ~111.19 km/degree of latitude
  }

  test("a known short distance is within a sane, hand-checkable range") {
    // ~0.25 degree grid spacing (ERA5 native resolution) at these latitudes is roughly 25-28 km
    val d = GeoDistance.haversineKm(13.0, 77.0, 13.25, 77.0)
    assert(d > 25.0 && d < 29.0)
  }
}
