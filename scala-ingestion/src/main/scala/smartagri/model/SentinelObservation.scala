package smartagri.model

import smartagri.spatial.GridCell
import smartagri.time.CanonicalDate

/**
 * One Sentinel-2-derived NDVI observation for a single grid cell on a
 * single date.
 *
 * Fields match exactly what the existing, working ingestion produces
 * today: the Python `Extract_ndvi.py` script, whose output --
 * `processed/sentinel2_ndvi.parquet`, columns
 * `[tile, date, cell_row, cell_col, lat, lon, ndvi]` -- is the canonical,
 * retained Sentinel-2 source per the reconciliation blueprint §16 (Python
 * is deliberately kept for this one ingestion step; the Scala
 * `SentinelIngestion.scala` alternative is non-functional -- it writes 0
 * rows, per the audit -- and additionally does not even carry `lat`/`lon`).
 * No fields have been invented or renamed here.
 *
 * `lat`/`lon` are the cell centroid coordinates *as established by this
 * grid-defining source*. Every other modality resolves its own samples
 * against these coordinates rather than computing an independent grid
 * (Phase-1 §3).
 *
 * A `ndviStd` (per-cell pixel standard deviation) field was proposed in
 * the Phase-1 blueprint as a future satellite-observation-quality signal
 * for the adaptive reliability model. It is intentionally NOT included
 * here: the existing ingestion code does not compute it today, and Task
 * 1E explicitly forbids inventing fields the existing project doesn't
 * actually support. It can be added once ingestion is extended to
 * compute it, in a later task.
 */
final case class SentinelObservation(
    cell: GridCell,
    date: CanonicalDate,
    lat: Double,
    lon: Double,
    /** `None` represents a missing/invalid NDVI reading -- never silently defaulted to 0 or any other value. */
    ndvi: Option[Double]
) {
  def canonicalKey: CanonicalKey = CanonicalKey(cell, date)
}

object SentinelObservation {
  /** Physically valid NDVI range. A present value outside this range is an error, not a value to clip/round. */
  val NdviMin: Double = -1.0
  val NdviMax: Double = 1.0

  /** Existing Parquet column names (`processed/sentinel2_ndvi.parquet`), unchanged from today's ingestion. */
  object Columns {
    val Ndvi = "ndvi"
    val Lat = "lat"
    val Lon = "lon"
  }
}
