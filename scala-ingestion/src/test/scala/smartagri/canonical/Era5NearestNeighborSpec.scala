package smartagri.canonical

import org.scalatest.funsuite.AnyFunSuite

class Era5NearestNeighborSpec extends AnyFunSuite {

  private val candidates = Seq(
    Era5Point(12.0, 76.0),
    Era5Point(12.0, 77.0),
    Era5Point(13.0, 77.0), // closest to (13.0, 77.1) below
    Era5Point(14.0, 78.0)
  )

  // -- 5. ERA5 nearest-neighbour selection ------------------------------

  test("nearest selects the genuinely closest candidate") {
    val (point, distance) = Era5NearestNeighbor.nearestWithDistance(13.0, 77.1, candidates)
    assert(point == Era5Point(13.0, 77.0))
    assert(distance > 0.0)
  }

  test("nearest returns exactly the point itself when it's in the candidate set") {
    val (point, distance) = Era5NearestNeighbor.nearestWithDistance(12.0, 76.0, candidates)
    assert(point == Era5Point(12.0, 76.0))
    assert(math.abs(distance) < 1e-9)
  }

  test("nearestWithDistance requires a non-empty candidate list") {
    assertThrows[IllegalArgumentException] {
      Era5NearestNeighbor.nearestWithDistance(13.0, 77.0, Seq.empty)
    }
  }

  // -- 6. Deterministic tie-breaking ------------------------------------

  test("an exact distance tie is broken by lowest latitude, then lowest longitude") {
    // Two points equidistant from the target by construction (symmetric around the target's latitude).
    val target = (13.0, 77.0)
    val tiedCandidates = Seq(
      Era5Point(12.5, 77.0), // 0.5 degrees south
      Era5Point(13.5, 77.0)  // 0.5 degrees north -- same distance from target by symmetry
    )
    val (winner, _) = Era5NearestNeighbor.nearestWithDistance(target._1, target._2, tiedCandidates)
    assert(winner == Era5Point(12.5, 77.0), "the lower-latitude point must win an exact tie")
  }

  test("a latitude tie is broken by lowest longitude") {
    val tiedCandidates = Seq(
      Era5Point(13.0, 77.5),
      Era5Point(13.0, 76.5)
    )
    // Both at the same latitude as a target directly between them in longitude only -- construct
    // a target equidistant in longitude too, so distances are identical and only lon breaks it.
    val (winner, _) = Era5NearestNeighbor.nearestWithDistance(13.0, 77.0, tiedCandidates)
    assert(winner == Era5Point(13.0, 76.5), "the lower-longitude point must win when latitudes tie")
  }

  test("tie-breaking is deterministic across repeated calls with the same input") {
    val target = (13.0, 77.0)
    val tiedCandidates = Seq(Era5Point(12.5, 77.0), Era5Point(13.5, 77.0))
    val results = (1 to 5).map(_ => Era5NearestNeighbor.nearest(target._1, target._2, tiedCandidates))
    assert(results.distinct.size == 1, "the same tie must resolve to the same winner every time")
  }
}
