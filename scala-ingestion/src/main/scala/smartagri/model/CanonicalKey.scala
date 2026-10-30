package smartagri.model

import smartagri.spatial.GridCell
import smartagri.time.CanonicalDate

/**
 * The authoritative multimodal join identity: canonical spatial key +
 * canonical analytical date -- `(tile, cell_row, cell_col, date)`.
 *
 * Every historical daily observation (Sentinel-2 today; the future
 * ERA5-sampled-to-cell table; and the future Kalman/ML/conformal/decision
 * tables) is expected to carry exactly these four columns under exactly
 * these names, so joins can be written once via `CanonicalKey.Columns.All`
 * instead of being re-typed by hand throughout the project (reconciliation
 * blueprint §3/§4, Phase-3 Task 1G).
 *
 * For IoT, the same `CanonicalKey` is used to join a reading into the
 * daily state, but it is *derived* from the reading's higher-frequency
 * `event_time` -- see `IotSensorReading.canonicalKey`. The raw
 * `event_time` itself is always preserved separately for
 * streaming/state-management logic; it is never discarded once the
 * derived date is computed.
 */
final case class CanonicalKey(cell: GridCell, date: CanonicalDate) {
  override def toString: String = s"$cell@${date.isoString}"
}

object CanonicalKey {

  /** Column names to join on, in a fixed order, wherever a canonical join is written. */
  object Columns {
    val All: Seq[String] = GridCell.Columns.All :+ CanonicalDate.ColumnName
  }

  /** Convenience constructor from raw, unvalidated strings/ints -- throws on invalid input. Prefer
    * `smartagri.validation.SchemaValidation` inside a data pipeline, where a bad row should be
    * reported rather than crash the job. */
  def apply(tile: String, cellRow: Int, cellCol: Int, date: String): CanonicalKey =
    CanonicalKey(GridCell(tile, cellRow, cellCol), CanonicalDate(date))
}
