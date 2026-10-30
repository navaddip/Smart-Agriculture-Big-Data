package smartagri.model

import org.scalatest.funsuite.AnyFunSuite
import smartagri.spatial.GridCell
import smartagri.time.CanonicalDate

class CanonicalKeySpec extends AnyFunSuite {

  test("canonical key generation from a GridCell and a CanonicalDate") {
    val key = CanonicalKey(GridCell("T43PHQ", 1, 2), CanonicalDate("2023-04-13"))
    assert(key.cell == GridCell("T43PHQ", 1, 2))
    assert(key.date == CanonicalDate("2023-04-13"))
    assert(key.toString == "T43PHQ:1:2@2023-04-13")
  }

  test("canonical key generation from raw strings/ints via the convenience constructor") {
    val key = CanonicalKey("T43PHQ", 1, 2, "2023-04-13")
    assert(key == CanonicalKey(GridCell("T43PHQ", 1, 2), CanonicalDate("2023-04-13")))
  }

  test("the convenience constructor throws on invalid components, same as the underlying types") {
    assertThrows[IllegalArgumentException](CanonicalKey("", 1, 2, "2023-04-13"))
    assertThrows[IllegalArgumentException](CanonicalKey("T43PHQ", 1, 2, "not-a-date"))
  }

  test("CanonicalKey.Columns.All is exactly the four canonical join columns, in order") {
    assert(CanonicalKey.Columns.All == Seq("tile", "cell_row", "cell_col", "date"))
  }

  test("two CanonicalKeys built from equal components are equal") {
    val a = CanonicalKey("T43PHQ", 5, 9, "2026-01-01")
    val b = CanonicalKey(GridCell("T43PHQ", 5, 9), CanonicalDate("2026-01-01"))
    assert(a == b)
  }
}
