"""
Novelty 3: uncertainty-aware prediction + resource optimization.

  * Trains a Random Forest crop-stress classifier, then wraps it in
    SPLIT-CONFORMAL prediction: a calibration set fixes a nonconformity
    threshold so the prediction SETS carry a guaranteed coverage level
    (default 90%). Uncertain samples yield the set {healthy, stressed}.
  * The irrigation policy then adds a safety margin scaled by that
    uncertainty: confident -> irrigate to the deficit; uncertain -> act
    conservatively. This closes the loop from calibrated uncertainty to action.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

FEATS = ["ph_water","nitrogen_g_kg","soil_organic_carbon","clay_pct","sand_pct",
         "silt_pct","water_content_fc","swvl1","swvl2","swvl3","swvl4",
         "t2m","d2m","tp","pev","e","ssrd","u10","v10"]


def conformal_sets(probs_cal, y_cal, probs_test, alpha=0.10):
    """Split-conformal: build prediction sets with >= 1-alpha coverage."""
    # nonconformity on calibration = 1 - prob assigned to the TRUE label
    true_prob = probs_cal[np.arange(len(y_cal)), y_cal]
    scores = 1.0 - true_prob
    n = len(scores)
    qlevel = min(1.0, np.ceil((n + 1) * (1 - alpha)) / n)
    qhat = np.quantile(scores, qlevel, method="higher")
    # a label is included in the set if its (1 - prob) <= qhat
    include = (1.0 - probs_test) <= qhat        # shape (n_test, 2)
    return include, qhat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", default="results")
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--alpha", type=float, default=0.10)
    args = ap.parse_args()
    Path(args.outdir).mkdir(exist_ok=True)

    df = pd.read_parquet(Path(args.processed) / "integrated_dataset.parquet")
    feats = [f for f in FEATS if f in df.columns]
    data = df.dropna(subset=feats + ["stressed"])
    X, y = data[feats].values, data["stressed"].values.astype(int)

    # train / calibration / test split (conformal needs a held-out calibration set)
    Xtr, Xtmp, ytr, ytmp = train_test_split(X, y, test_size=0.4, random_state=42, stratify=y)
    Xcal, Xte, ycal, yte = train_test_split(Xtmp, ytmp, test_size=0.5, random_state=42, stratify=ytmp)

    rf = RandomForestClassifier(n_estimators=200, max_depth=12, random_state=42,
                                n_jobs=-1, class_weight="balanced")
    rf.fit(Xtr, ytr)

    include, qhat = conformal_sets(rf.predict_proba(Xcal), ycal,
                                   rf.predict_proba(Xte), alpha=args.alpha)

    # evaluate conformal guarantees
    covered = include[np.arange(len(yte)), yte].mean()
    set_size = include.sum(axis=1)
    uncertain = (set_size == 2).mean()
    print(f"Target coverage: {1-args.alpha:.0%}   |   Empirical coverage: {covered:.1%}")
    print(f"Mean prediction-set size: {set_size.mean():.2f}")
    print(f"Flagged uncertain (set = both classes): {uncertain:.1%}")

    # --- uncertainty-aware irrigation policy ---
    te = data.iloc[-len(yte):].copy()
    te["set_size"] = set_size
    te["deficit"] = (te["water_content_fc"]/100.0 - te["swvl1"]).clip(lower=0)
    base_irr = te["deficit"] * 100
    # safety margin: +30% water when the model is uncertain about stress
    te["irrigation_mm"] = (base_irr * (1 + 0.30 * (te["set_size"] == 2))).round(1)
    print(f"\nMean irrigation (uncertainty-aware): {te['irrigation_mm'].mean():.1f} mm")
    print(f"  vs baseline (no margin):           {base_irr.mean():.1f} mm")

    # figure: prediction-set composition
    fig, ax = plt.subplots(1, 2, figsize=(11, 4))
    labels = ["confident\n(1 label)", "uncertain\n(both)"]
    ax[0].bar(labels, [np.mean(set_size == 1), np.mean(set_size == 2)],
              color=["#2e7d32", "#f9a825"])
    ax[0].set_ylabel("fraction of predictions")
    ax[0].set_title(f"Conformal prediction sets (coverage {covered:.0%})")
    ax[1].hist(te["irrigation_mm"], bins=30, color="#1565c0")
    ax[1].set_xlabel("Recommended irrigation (mm)"); ax[1].set_ylabel("cells")
    ax[1].set_title("Uncertainty-aware irrigation")
    plt.tight_layout()
    plt.savefig(Path(args.outdir) / "conformal_irrigation.png", dpi=130)
    print(f"\nFigure -> {args.outdir}/conformal_irrigation.png")


if __name__ == "__main__":
    main()
