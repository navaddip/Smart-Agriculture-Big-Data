"""
Reliability-aware Kalman filter for continuous crop-state estimation.
Implements Novelties 1 + 2 as one mechanism:

  * State x_t = latent crop vigour (NDVI scale), evolved daily by a
    weather-driven process model (mean reversion toward a soil-moisture /
    temperature equilibrium).
  * Two measurement sources are fused by inverse-variance (reliability)
    weighting: sparse-but-accurate satellite NDVI, and a dense-but-noisy
    IoT soil-moisture-derived proxy. Reliability sets each source's variance.
  * Between satellite passes the filter runs pure prediction, so it emits a
    CONTINUOUS daily estimate with an uncertainty band (posterior variance P).

Outputs a per-field daily estimate table plus a demonstration figure.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# --- filter parameters (documented so they go straight into the paper) ---
ALPHA = 0.05      # process mean-reversion rate toward weather equilibrium
Q = 0.0008        # process noise variance (uncertainty growth per day)
R_SAT = 0.0025    # satellite measurement variance (accurate)
R_IOT = 0.02      # IoT proxy measurement variance (noisier)
IOT_STALE = 0.01  # extra IoT variance added per day since last reading
SWVL_REF = 0.25   # reference soil moisture for the equilibrium model


def weather_equilibrium(swvl1, t2m):
    """Weather-driven equilibrium vigour: wetter -> greener, heat -> stressed."""
    eq = 0.45 + 1.2 * (swvl1 - SWVL_REF) - 0.02 * np.maximum(0.0, t2m - 30.0)
    return float(np.clip(eq, 0.0, 0.9))


def iot_proxy(swvl_sensor):
    """Map an IoT soil-moisture reading to a crude vigour proxy."""
    return float(np.clip(0.15 + 1.0 * swvl_sensor, 0.0, 0.9))


def run_filter(daily):
    """Run the reliability-aware Kalman filter over one field's daily timeline."""
    x, P = 0.4, 0.05          # initial state and uncertainty
    days_since_iot = 0
    est, unc = [], []
    for _, row in daily.iterrows():
        # --- predict: weather drives the state, uncertainty grows ---
        eq = weather_equilibrium(row["swvl1"], row["t2m"])
        x = x + ALPHA * (eq - x)
        P = P + Q

        # --- gather available measurements and their reliabilities ---
        z_list, r_list = [], []
        if not np.isnan(row["ndvi"]):                       # satellite present
            z_list.append(row["ndvi"]);  r_list.append(R_SAT)
        if not np.isnan(row["iot_sensor"]):                 # IoT present (dense)
            r_iot = R_IOT + IOT_STALE * days_since_iot
            z_list.append(iot_proxy(row["iot_sensor"]));  r_list.append(r_iot)
            days_since_iot = 0
        else:
            days_since_iot += 1

        # --- reliability-weighted (inverse-variance) fusion + update ---
        if z_list:
            inv = [1.0 / r for r in r_list]
            z_fused = sum(z * w for z, w in zip(z_list, inv)) / sum(inv)
            r_fused = 1.0 / sum(inv)
            K = P / (P + r_fused)
            x = x + K * (z_fused - x)
            P = (1 - K) * P
        est.append(x); unc.append(P)
    daily = daily.copy()
    daily["vigour_est"] = est
    daily["vigour_var"] = unc
    return daily


def build_field_timeline(cell_ndvi, weather, start, end):
    """Daily frame for one field: weather + (sparse) NDVI + synthetic IoT sensor."""
    days = pd.DataFrame({"date": pd.date_range(start, end)})
    days["date"] = days["date"].dt.strftime("%Y-%m-%d")
    df = days.merge(weather, on="date", how="left")
    df = df.merge(cell_ndvi[["date", "ndvi"]], on="date", how="left")
    # synthetic IoT soil-moisture sensor: daily, = ERA5 swvl1 + sensor noise
    rng = np.random.default_rng(0)
    df["iot_sensor"] = df["swvl1"] + rng.normal(0, 0.02, len(df))
    df = df.dropna(subset=["swvl1", "t2m"]).reset_index(drop=True)
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="processed")
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--max_fields", type=int, default=800, help="fields to process")
    args = ap.parse_args()
    Path(args.outdir).mkdir(exist_ok=True)

    ndvi = pd.read_parquet(Path(args.processed) / "sentinel2_ndvi.parquet")
    era5 = pd.read_parquet(Path(args.processed) / "era5_weather.parquet")
    start, end = ndvi["date"].min(), ndvi["date"].max()

    # snap ERA5 to 0.25 deg keys for nearest-cell weather lookup
    era5 = era5.copy()
    era5["lat_r"] = (era5["lat"] * 4).round() / 4
    era5["lon_r"] = (era5["lon"] * 4).round() / 4

    # rank fields by how many satellite observations they have (best for demo)
    counts = (ndvi.groupby(["tile", "cell_row", "cell_col"])
                  .size().sort_values(ascending=False))
    fields = counts.index[: args.max_fields]

    all_out, demo = [], []
    for i, key in enumerate(fields):
        tile, r, c = key
        cell = ndvi[(ndvi.tile == tile) & (ndvi.cell_row == r) & (ndvi.cell_col == c)]
        lat_r = round(cell["lat"].iloc[0] * 4) / 4
        lon_r = round(cell["lon"].iloc[0] * 4) / 4
        w = era5[(era5.lat_r == lat_r) & (era5.lon_r == lon_r)][
            ["date", "swvl1", "t2m"]].drop_duplicates("date")
        if w.empty:
            continue
        tl = build_field_timeline(cell, w, start, end)
        if tl.empty:
            continue
        out = run_filter(tl)
        out["tile"], out["cell_row"], out["cell_col"] = tile, r, c
        all_out.append(out[["tile", "cell_row", "cell_col", "date",
                            "vigour_est", "vigour_var", "ndvi"]])
        if len(demo) < 4:
            demo.append((key, out))

    result = pd.concat(all_out, ignore_index=True)
    result.to_parquet(Path(args.outdir) / "crop_state_daily.parquet", index=False)
    print(f"Continuous crop-state written: {len(result):,} field-days "
          f"across {len(all_out)} fields")

    # --- demonstration figure: continuous estimate vs sparse observations ---
    fig, axes = plt.subplots(len(demo), 1, figsize=(10, 2.4 * len(demo)), sharex=True)
    if len(demo) == 1:
        axes = [axes]
    for ax, (key, out) in zip(axes, demo):
        d = pd.to_datetime(out["date"])
        sd = np.sqrt(out["vigour_var"].values)
        ax.fill_between(d, out.vigour_est - 2 * sd, out.vigour_est + 2 * sd,
                        color="#2e7d32", alpha=0.15, label="±2σ uncertainty")
        ax.plot(d, out.vigour_est, color="#2e7d32", lw=1.5,
                label="Kalman estimate (continuous)")
        obs = out.dropna(subset=["ndvi"])
        ax.scatter(pd.to_datetime(obs.date), obs.ndvi, color="#c62828", s=30,
                   zorder=5, label="Satellite NDVI (sparse)")
        ax.set_ylabel("vigour"); ax.set_ylim(0, 1)
        ax.set_title(f"Field {key}", fontsize=9)
    axes[0].legend(fontsize=8, loc="upper right")
    plt.suptitle("Reliability-aware continuous crop-state estimation", y=1.0)
    plt.tight_layout()
    plt.savefig(Path(args.outdir) / "crop_state_estimation.png", dpi=130,
                bbox_inches="tight")
    print(f"Demo figure -> {args.outdir}/crop_state_estimation.png")


if __name__ == "__main__":
    main()
