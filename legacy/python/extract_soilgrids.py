"""
Stage 1 preprocessing: SoilGrids GeoTIFFs  ->  tabular soil Parquet.

Soil properties are static (no time dimension). We sample each GeoTIFF at the
SAME grid-cell centres used by the NDVI table, so all three modalities share
one grid and join cleanly. Output is one row per grid cell:

    tile, cell_row, cell_col, lat, lon, <one column per soil property>

The grid (cell -> lat/lon) is read from the NDVI Parquet, guaranteeing
alignment. Raw SoilGrids integers are converted to conventional units, and
nodata pixels become NaN.

Usage:
    python extract_soilgrids.py \
        --tif_dir "D:/SEM-5/BIG_DATA/data/soilgrids" \
        --grid    "D:/SEM-5/BIG_DATA/processed/sentinel2_ndvi.parquet" \
        --output  "D:/SEM-5/BIG_DATA/processed/soilgrids.parquet"
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer

# Match a filename (lowercased) to a clean column name and a SoilGrids
# conversion factor (raw integer / factor -> conventional unit).
# Keyword list is tolerant of the typos in the downloaded filenames
# (e.g. "Nirtogen", "slit", "watevol").
PROPERTY_RULES = [
    ("ph",        "ph_water",             10.0),   # pH*10  -> pH
    ("nir",       "nitrogen_g_kg",       100.0),   # cg/kg  -> g/kg   (matches "Nirtogen")
    ("nitro",     "nitrogen_g_kg",       100.0),
    ("organic",   "soil_organic_carbon", 10.0),    # dg/kg  -> g/kg
    ("carbon",    "soil_organic_carbon", 10.0),
    ("clay",      "clay_pct",            10.0),     # g/kg   -> %
    ("sand",      "sand_pct",            10.0),
    ("slit",      "silt_pct",            10.0),     # typo for silt
    ("silt",      "silt_pct",            10.0),
    ("wat",       "water_content_fc",    10.0),     # vol water at field capacity
    ("vol",       "water_content_fc",    10.0),
]


def classify(filename):
    """Return (column_name, conversion_factor) for a soil GeoTIFF filename."""
    low = filename.lower()
    for keyword, col, factor in PROPERTY_RULES:
        if keyword in low:
            return col, factor
    # Fallback: sanitise the stem, no conversion.
    clean = "".join(c if c.isalnum() else "_" for c in Path(filename).stem).strip("_").lower()
    return clean, 1.0


def sample_raster(tif_path, lats, lons):
    """Sample a GeoTIFF at the given lat/lon points; nodata -> NaN, apply unit factor."""
    with rasterio.open(tif_path) as ds:
        to_crs = Transformer.from_crs("EPSG:4326", ds.crs, always_xy=True)
        xs, ys = to_crs.transform(lons, lats)
        vals = np.array([v[0] for v in ds.sample(zip(xs, ys))], dtype="float64")
        nodata = ds.nodata
    if nodata is not None:
        vals[vals == nodata] = np.nan
    # 0 = outside soil-tile coverage; no soil property is truly 0. Applied UNCONDITIONALLY (not
    # only when `nodata` metadata exists) -- Task 3A's audit found that some SoilGrids GeoTIFF
    # exports omit a nodata tag entirely, which previously let raw zero-valued nodata pixels flow
    # straight through unmasked (8,522 of 19,926 output rows ended up with all seven soil
    # properties == 0.0 instead of null; repaired in the existing output by
    # smartagri.canonical.SoilGridsNodataRepair, but that repair can't help a *future*
    # re-extraction unless this line also runs unconditionally, which is the fix here).
    vals[vals == 0] = np.nan
    return vals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tif_dir", required=True, help="folder of SoilGrids .tif files")
    ap.add_argument("--grid", required=True, help="NDVI parquet, used for the cell grid")
    ap.add_argument("--output", required=True, help="output .parquet path")
    args = ap.parse_args()

    # Canonical grid: unique cells with their lat/lon, taken from the NDVI table.
    ndvi = pd.read_parquet(args.grid, columns=["tile", "cell_row", "cell_col", "lat", "lon"])
    grid = ndvi.drop_duplicates(subset=["tile", "cell_row", "cell_col"]).reset_index(drop=True)
    print(f"Grid has {len(grid):,} cells")

    lats = grid["lat"].to_numpy()
    lons = grid["lon"].to_numpy()

    tifs = sorted(Path(args.tif_dir).glob("*.tif"))
    if not tifs:
        raise SystemExit(f"No .tif files found in {args.tif_dir}")

    for tif in tifs:
        col, factor = classify(tif.name)
        print(f"  {tif.name}  ->  {col}  (/{factor:g})")
        grid[col] = sample_raster(str(tif), lats, lons) / factor

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    grid.to_parquet(args.output, index=False)

    soil_cols = [c for c in grid.columns if c not in ("tile", "cell_row", "cell_col", "lat", "lon")]
    print(f"\nWrote {len(grid):,} rows -> {args.output}")
    print(grid[soil_cols].describe())


if __name__ == "__main__":
    main()