package smartagri.spatial

import org.scalatest.funsuite.AnyFunSuite

class GridCellSpec extends AnyFunSuite {

  test("a valid GridCell constructs successfully and exposes its fields") {
    val cell = GridCell("T43PHQ", 12, 87)
    assert(cell.tile == "T43PHQ")
    assert(cell.cellRow == 12)
    assert(cell.cellCol == 87)
  }

  test("GridCell.toString renders the stable tile:row:col identity") {
    val cell = GridCell("T43PHQ", 12, 87)
    assert(cell.toString == "T43PHQ:12:87")
    assert(cell.asKafkaKey == "T43PHQ:12:87")
  }

  test("an invalid GridCell (empty tile) throws via the primary constructor") {
    assertThrows[IllegalArgumentException](GridCell("", 0, 0))
    assertThrows[IllegalArgumentException](GridCell("   ", 0, 0))
    assertThrows[IllegalArgumentException](GridCell(null, 0, 0))
  }

  test("an invalid GridCell (negative row/col) throws via the primary constructor") {
    assertThrows[IllegalArgumentException](GridCell("T43PHQ", -1, 0))
    assertThrows[IllegalArgumentException](GridCell("T43PHQ", 0, -1))
  }

  test("GridCell.validated returns Right for valid input, without throwing") {
    val result = GridCell.validated("T43PHQ", 0, 0)
    assert(result.isRight)
    assert(result == Right(GridCell("T43PHQ", 0, 0)))
  }

  test("GridCell.validated returns Left with a message for invalid input, without throwing") {
    val result = GridCell.validated("", -1, -1)
    assert(result.isLeft)
    assert(result.left.getOrElse("").nonEmpty)
  }

  test("two GridCells with the same fields are equal and share a hash code (case-class equality)") {
    val a = GridCell("T43PHQ", 1, 2)
    val b = GridCell("T43PHQ", 1, 2)
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }

  test("GridCell.Columns names are the canonical, non-abbreviated column names") {
    assert(GridCell.Columns.Tile == "tile")
    assert(GridCell.Columns.CellRow == "cell_row")
    assert(GridCell.Columns.CellCol == "cell_col")
    assert(GridCell.Columns.All == Seq("tile", "cell_row", "cell_col"))
  }

  test("GridCell.asTuple round-trips into an equal GridCell") {
    val cell = GridCell("T43PHQ", 5, 9)
    val (tile, row, col) = cell.asTuple
    assert(GridCell(tile, row, col) == cell)
  }
}
