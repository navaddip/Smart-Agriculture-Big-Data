"""
Stage 1 preprocessing: ERA5 GRIB  ->  daily tabular weather Parquet.

Reads the ERA5 single-level GRIB (unzipping the CDS download automatically),
discovers whatever weather variables it contains, and aggregates the sub-daily
readings to one row per (date, lat, lon):

    date, lat, lon, <one column per weather variable>

Instantaneous variables (temperature, soil moisture, wind) are averaged over
the day; accumulation-type variables (precipitation, radiation, evaporation)
are summed. Temperatures are converted K -> degrees C and precipitation m -> mm
for readability.

Usage:
    pip install cfgrib xarray pandas pyarrow
    python extract_era5.py --input  "D:/SEM-5/BIG_DATA/data/era5" \
                           --output "D:/SEM-5/BIG_DATA/processed/era5_weather.parquet"

If cfgrib fails to find the eccodes binary on Windows, use conda instead:
    conda install -c conda-forge cfgrib xarray pyarrow
"""

import argparse
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import xarray as xr

# ERA5 short names that are accumulations -> summed over the day, not averaged.
SUM_VARS = {"tp", "ssrd", "ssr", "strd", "str", "e", "pev", "ro", "sf"}
# Variables to convert from Kelvin to Celsius.
KELVIN_VARS = {"t2m", "d2m", "skt", "stl1", "stl2", "stl3", "stl4"}
# Variables to convert from metres to millimetres.
METRE_VARS = {"tp", "e", "pev", "sf", "ro"}


def find_grib(input_path):
    """Return a path to a .grib file, unzipping the CDS download to temp if needed."""
    p = Path(input_path)
    candidates = []
    if p.is_dir():
        candidates = list(p.glob("*.grib")) + list(p.glob("*.grib2")) + list(p.glob("*.zip"))
    else:
        candidates = [p]
    if not candidates:
        raise SystemExit(f"No .grib or .zip found in {input_path}")

    target = candidates[0]
    if target.suffix == ".zip":
        tmpdir = Path(tempfile.mkdtemp())
        with zipfile.ZipFile(target) as zf:
            inner = [n for n in zf.namelist() if n.endswith((".grib", ".grib2")) or "grib" in n.lower()]
            if not inner:
                raise SystemExit(f"No GRIB file inside {target.name}")
            zf.extract(inner[0], tmpdir)
            print(f"Unzipped {inner[0]} from {target.name}")
            return tmpdir / inner[0]
    return target


def grib_to_frame(grib_path):
    """Open the GRIB (possibly several internal datasets) and return a long DataFrame."""
    # open_datasets returns a list because ERA5 mixes instantaneous and accumulated
    # fields that cannot share a single hypercube.
    datasets = xr.open_dataset  # placeholder to keep linters calm
    import cfgrib
    dss = cfgrib.open_datasets(str(grib_path))
    print(f"GRIB contains {len(dss)} internal dataset(s)")

    merged = None
    for ds in dss:
        df = ds.to_dataframe().reset_index()
        time_col = "valid_time" if "valid_time" in df.columns else "time"
        keep_coords = [time_col, "latitude", "longitude"]
        data_cols = [c for c in ds.data_vars]
        sub = df[keep_coords + data_cols].rename(
            columns={time_col: "time", "latitude": "lat", "longitude": "lon"}
        )
        merged = sub if merged is None else merged.merge(sub, on=["time", "lat", "lon"], how="outer")
    return merged


def aggregate_daily(df):
    """Collapse sub-daily readings to one row per (date, lat, lon)."""
    df["date"] = pd.to_datetime(df["time"]).dt.date
    value_cols = [c for c in df.columns if c not in ("time", "date", "lat", "lon")]

    agg_map = {c: ("sum" if c in SUM_VARS else "mean") for c in value_cols}
    daily = df.groupby(["date", "lat", "lon"], as_index=False).agg(agg_map)

    for c in value_cols:
        if c in KELVIN_VARS:
            daily[c] = daily[c] - 273.15
        if c in METRE_VARS:
            daily[c] = daily[c] * 1000.0
    return daily


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="era5 folder, .zip, or .grib file")
    ap.add_argument("--output", required=True, help="output .parquet path")
    args = ap.parse_args()

    grib_path = find_grib(args.input)
    print(f"Reading {grib_path}")
    frame = grib_to_frame(grib_path)
    daily = aggregate_daily(frame)

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(args.output, index=False)

    cols = [c for c in daily.columns if c not in ("date", "lat", "lon")]
    print(f"\nWrote {len(daily):,} rows -> {args.output}")
    print(f"Weather variables found: {cols}")
    print(daily.head())


if __name__ == "__main__":
    main()