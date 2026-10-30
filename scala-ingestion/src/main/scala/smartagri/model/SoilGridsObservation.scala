package smartagri.model

import smartagri.spatial.GridCell

/**
 * One SoilGrids-derived static soil observation for a single grid cell.
 *
 * No `date` field: SoilGrids is treated as a single static snapshot by
 * both existing ingestion implementations -- Python `extract_soilgrids.py`
 * (`processed/soilgrids.parquet`) and Scala `SoilGridsIngestion.scala`
 * (`processed/soilgrids_scala.parquet`) -- and both already agree on this
 * exact column set: `[tile, cell_row, cell_col, lat, lon, ph_water,
 * nitrogen_g_kg, soil_organic_carbon, clay_pct, sand_pct, silt_pct,
 * water_content_fc]`.
 *
 * (The audit found a *third*, legacy Scala implementation --
 * `scala/soilgrids.scala` -- that duplicates this and writes to a
 * colliding output path. That duplicate is not touched in this task; see
 * `docs/migration/legacy-compatibility-mapping.md`.)
 *
 * Every field below is optional because the existing ingestion nulls out
 * out-of-coverage or zero/invalid raster samples rather than defaulting
 * them (`SoilGridsIngestion.scala` only writes a value when the sampled
 * raster pixel is non-NaN and non-zero) -- that missingness must be
 * preserved here too, not silently replaced (Task 1H / Veracity).
 */
final case class SoilGridsObservation(
    cell: GridCell,
    lat: Double,
    lon: Double,
    phWater: Option[Double],
    nitrogenGKg: Option[Double],
    soilOrganicCarbon: Option[Double],
    clayPct: Option[Double],
    sandPct: Option[Double],
    siltPct: Option[Double],
    waterContentFc: Option[Double]
)

object SoilGridsObservation {
  /** Existing Parquet column names, unchanged from today's ingestion (both Python and Scala agree on these). */
  object Columns {
    val Lat = "lat"
    val Lon = "lon"
    val PhWater = "ph_water"
    val NitrogenGKg = "nitrogen_g_kg"
    val SoilOrganicCarbon = "soil_organic_carbon"
    val ClayPct = "clay_pct"
    val SandPct = "sand_pct"
    val SiltPct = "silt_pct"
    val WaterContentFc = "water_content_fc"
  }
}
