package smartagri.model

import smartagri.time.CanonicalDate

/**
 * One ERA5 daily weather observation at a native ERA5 grid point
 * (`lat`/`lon`), for a single canonical `date`.
 *
 * IMPORTANT -- this model deliberately does NOT carry a `GridCell`. ERA5's
 * native resolution (~0.25 degrees) is much coarser than the Sentinel-2
 * grid, and the existing, working pipeline (`kalman_fusion.py`) samples
 * ERA5 at the *nearest* ERA5 lat/lon point to each cell centroid
 * downstream, rather than pre-joining ERA5 to cells at ingestion time.
 * Forcing a `GridCell` onto this model now would invent a join the
 * existing project does not actually perform (Task 1E: do not fabricate
 * fields/relationships beyond what's supported).
 *
 * REQUIRED FUTURE CONVERSION (not implemented here -- documented per the
 * Task 1 correction pass, Correction 2):
 *
 *     ERA5Observation (native ERA5 lat/lon grid)
 *             |
 *             v
 *     spatial sampling / interpolation
 *             |
 *             v
 *     canonical Sentinel field grid (the grid SentinelObservation defines)
 *             |
 *             v
 *     GridCell (tile, cell_row, cell_col) + CanonicalDate
 *
 * This conversion MUST happen before ERA5 participates in the historical
 * multimodal join (the future join keyed on `CanonicalKey`, §Task 1G).
 * The future implementation performing this sampling step must resolve
 * against the ONE canonical grid that `SentinelObservation`/`GridCell`
 * already define -- it must NOT independently invent a second ERA5-native
 * grid (e.g. its own row/col indexing over ERA5's lat/lon points). Doing
 * so would recreate exactly the kind of duplicate-grid inconsistency the
 * canonical spatial key (Task 1B) exists to prevent.
 *
 * Fields match exactly the existing, working Python ingestion output
 * (`processed/era5_weather.parquet`, columns
 * `[date, lat, lon, swvl1, swvl2, swvl3, swvl4, u10, v10, t2m, d2m,
 * ssrd, e, tp, pev]`). The current Scala `Era5Ingestion.scala` module
 * emits a different, long-format table keyed by an opaque `time_index`
 * instead of `date`, and does not yet conform to this canonical model --
 * see `docs/migration/legacy-compatibility-mapping.md` for the tracked
 * gap; it is not fixed in this task.
 *
 * Field meanings (standard ERA5 short names, left exactly as the existing
 * data uses them, per Task 1E -- not renamed even though some are
 * terse, since a silent rename would risk hiding which existing pipeline
 * field a name refers to):
 *   - swvl1..swvl4: volumetric soil water, layers 1-4
 *   - u10, v10:     10m wind components (eastward, northward)
 *   - t2m, d2m:     2m air temperature, 2m dewpoint temperature
 *   - ssrd:         surface solar radiation downwards
 *   - e:            evaporation
 *   - tp:           total precipitation
 *   - pev:          potential evaporation
 */
final case class Era5Observation(
    date: CanonicalDate,
    lat: Double,
    lon: Double,
    swvl1: Option[Double],
    swvl2: Option[Double],
    swvl3: Option[Double],
    swvl4: Option[Double],
    u10: Option[Double],
    v10: Option[Double],
    t2m: Option[Double],
    d2m: Option[Double],
    ssrd: Option[Double],
    e: Option[Double],
    tp: Option[Double],
    pev: Option[Double]
)

object Era5Observation {
  /** Existing Parquet column names (`processed/era5_weather.parquet`), unchanged from today's ingestion. */
  object Columns {
    val Lat = "lat"
    val Lon = "lon"
    val All: Seq[String] = Seq(
      CanonicalDate.ColumnName, Lat, Lon,
      "swvl1", "swvl2", "swvl3", "swvl4",
      "u10", "v10", "t2m", "d2m", "ssrd", "e", "tp", "pev"
    )
  }
}
