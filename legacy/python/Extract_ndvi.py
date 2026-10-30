"""
Stage 1 preprocessing: Sentinel-2 L2A .SAFE zips  ->  tabular NDVI Parquet.

For each scene we read the 10 m red (B04) and near-infrared (B08) bands,
compute per-pixel NDVI, and aggregate it to a coarse GRID x GRID mesh of
"fields" (default 100 x 100). Each output row is one grid cell in one scene:

    tile, date, cell_row, cell_col, lat, lon, ndvi

This tabular form is what Spark reads downstream. Rasters are decoded here
in Python because Spark has no native .jp2 decoder.

Usage:
    pip install rasterio numpy pandas pyarrow pyproj
    python extract_ndvi.py --input  "D:/SEM-5/BIG_DATA/data/sentinel2" \
                           --output "D:/SEM-5/BIG_DATA/processed/sentinel2_ndvi.parquet"
    # try one scene first:
    python extract_ndvi.py --input ... --output ... --limit 1
"""

import argparse
import re
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer

# Sentinel-2 L2A radiometric offset for processing baseline >= N0400.
# Your files are N0510, so the offset applies: reflectance = (DN - 1000) / 10000.
# NDVI is a ratio so the 1/10000 scale cancels, but the additive offset does not,
# so we apply it before computing NDVI.
BOA_OFFSET = -1000
GRID = 100          # output mesh is GRID x GRID cells per tile
READ_FACTOR = 10    # read each band at GRID*READ_FACTOR resolution, then block-average
FULL_RES = 10980    # native 10 m tile size in pixels


def find_band(zip_names, suffix):
    """Locate the inner path of a 10 m band (e.g. '..._B04_10m.jp2') inside the zip."""
    for name in zip_names:
        if name.endswith(suffix):
            return name
    raise FileNotFoundError(f"No band ending in {suffix} found in archive")


def read_band(zip_path_posix, inner_path, out_size):
    """Read one band, decimated to out_size x out_size, as float with 0 -> NaN."""
    vsi = f"/vsizip/{zip_path_posix}/{inner_path}"
    with rasterio.open(vsi) as ds:
        arr = ds.read(
            1,
            out_shape=(out_size, out_size),
            resampling=rasterio.enums.Resampling.nearest,
        ).astype("float32")
        transform = ds.transform   # full-resolution affine transform
        crs = ds.crs
    arr[arr == 0] = np.nan         # 0 is the Sentinel-2 nodata value
    return arr, transform, crs


def block_mean(arr, factor):
    """Reduce a (GRID*factor, GRID*factor) array to (GRID, GRID) via nan-aware mean."""
    g = arr.shape[0] // factor
    reshaped = arr.reshape(g, factor, g, factor)
    with np.errstate(invalid="ignore"):
        return np.nanmean(reshaped, axis=(1, 3))


def cell_latlon(transform, crs, grid):
    """Lat/lon of each grid-cell centre, in EPSG:4326."""
    step = FULL_RES / grid
    rows = (np.arange(grid) + 0.5) * step
    cols = (np.arange(grid) + 0.5) * step
    cc, rr = np.meshgrid(cols, rows)          # cc = columns, rr = rows
    x = transform.c + transform.a * cc + transform.b * rr
    y = transform.f + transform.d * cc + transform.e * rr
    to_wgs84 = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    lon, lat = to_wgs84.transform(x, y)
    return lat, lon


def parse_name(zip_path):
    """Pull the tile id (T43PGQ) and sensing date (YYYY-MM-DD) from the filename."""
    stem = zip_path.stem
    tile = re.search(r"_(T\d{2}[A-Z]{3})_", stem)
    date = re.search(r"_(\d{8})T", stem)
    tile = tile.group(1) if tile else "unknown"
    date = date.group(1) if date else "unknown"
    if date != "unknown":
        date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    return tile, date


def process_scene(zip_path):
    """Turn one .SAFE zip into a DataFrame of grid-cell NDVI rows."""
    tile, date = parse_name(zip_path)
    zip_posix = zip_path.as_posix()
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    b04_name = find_band(names, "B04_10m.jp2")
    b08_name = find_band(names, "B08_10m.jp2")

    read_size = GRID * READ_FACTOR
    red, transform, crs = read_band(zip_posix, b04_name, read_size)
    nir, _, _ = read_band(zip_posix, b08_name, read_size)

    red = red + BOA_OFFSET
    nir = nir + BOA_OFFSET
    with np.errstate(invalid="ignore", divide="ignore"):
        ndvi = (nir - red) / (nir + red)
    ndvi[np.abs(nir + red) < 1e-6] = np.nan
    ndvi[(ndvi < -1) | (ndvi > 1)] = np.nan

    ndvi_grid = block_mean(ndvi, READ_FACTOR)
    lat, lon = cell_latlon(transform, crs, GRID)

    rows, cols = np.meshgrid(np.arange(GRID), np.arange(GRID), indexing="ij")
    df = pd.DataFrame({
        "tile": tile,
        "date": date,
        "cell_row": rows.ravel(),
        "cell_col": cols.ravel(),
        "lat": lat.ravel(),
        "lon": lon.ravel(),
        "ndvi": ndvi_grid.ravel(),
    })
    return df.dropna(subset=["ndvi"]).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="folder of Sentinel-2 .zip files")
    ap.add_argument("--output", required=True, help="output .parquet path")
    ap.add_argument("--limit", type=int, default=None, help="process only N scenes")
    args = ap.parse_args()

    zips = sorted(Path(args.input).glob("*.zip"))
    if args.limit:
        zips = zips[: args.limit]
    if not zips:
        raise SystemExit(f"No .zip files found in {args.input}")

    frames = []
    for i, z in enumerate(zips, 1):
        print(f"[{i}/{len(zips)}] {z.name}")
        try:
            frames.append(process_scene(z))
        except Exception as e:
            print(f"    skipped ({e})")

    out = pd.concat(frames, ignore_index=True)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.output, index=False)
    print(f"\nWrote {len(out):,} rows from {len(frames)} scenes -> {args.output}")


if __name__ == "__main__":
    main()