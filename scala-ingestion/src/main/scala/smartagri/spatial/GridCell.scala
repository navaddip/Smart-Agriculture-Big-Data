package smartagri.spatial

/**
 * The single canonical spatial identity for every modality in this project.
 *
 * A `GridCell` identifies one cell of the fixed grid established by the
 * Sentinel-2 ingestion step for a given MGRS tile (`tile`, e.g. "T43PHQ").
 * Every other historical or streaming modality (SoilGrids sampling, the
 * future ERA5-to-cell sampling, IoT sensor placement) must resolve to a
 * `GridCell` using exactly these field/column names -- `tile`, `cell_row`,
 * `cell_col` -- and never an abbreviation (`cr`/`cc`) or an alternate name
 * (`row`/`col`). That inconsistency (`cr`/`cc` vs `cell_row`/`cell_col`) is
 * what broke Stage 4 of the existing `Analytics.scala` job; this type
 * exists specifically to make that class of bug impossible going forward.
 *
 * Legacy code (`Analytics.scala`'s `FI`/`FO` case classes, which use
 * `cr`/`cc`) is NOT rewritten by this task -- see
 * `docs/migration/legacy-compatibility-mapping.md` for the tracked
 * migration mapping.
 *
 * @param tile    Sentinel-2 MGRS tile identifier, e.g. "T43PHQ". Must be non-empty.
 * @param cellRow Row index into the tile's fixed grid. Must be >= 0.
 * @param cellCol Column index into the tile's fixed grid. Must be >= 0.
 */
final case class GridCell(tile: String, cellRow: Int, cellCol: Int) {
  require(tile != null && tile.trim.nonEmpty, "GridCell.tile must not be empty")
  require(cellRow >= 0, s"GridCell.cellRow must be >= 0, was $cellRow")
  require(cellCol >= 0, s"GridCell.cellCol must be >= 0, was $cellCol")

  /** Stable, human-readable identity, e.g. "T43PHQ:12:87". Safe to use as a map/log key. */
  override def toString: String = s"$tile:$cellRow:$cellCol"

  /** Kafka-message-key form for the future IoT stream (reconciliation blueprint §5/§13). */
  def asKafkaKey: String = toString

  /** Plain tuple form, convenient for `Dataset.groupByKey` and similar Spark APIs. */
  def asTuple: (String, Int, Int) = (tile, cellRow, cellCol)
}

object GridCell {

  /**
   * Column names to use verbatim on every Spark DataFrame/Dataset that
   * carries spatial identity. Referencing these constants instead of
   * writing string literals throughout the project is what actually
   * prevents the `cr`/`cc` drift from recurring -- see the class doc.
   */
  object Columns {
    val Tile: String = "tile"
    val CellRow: String = "cell_row"
    val CellCol: String = "cell_col"
    val All: Seq[String] = Seq(Tile, CellRow, CellCol)
  }

  /**
   * Non-throwing constructor for use inside data-processing pipelines,
   * where a malformed row should become a reported error rather than an
   * uncaught exception that kills the job (Task 1H: report, don't crash).
   */
  def validated(tile: String, cellRow: Int, cellCol: Int): Either[String, GridCell] =
    try Right(GridCell(tile, cellRow, cellCol))
    catch { case e: IllegalArgumentException => Left(e.getMessage) }
}
