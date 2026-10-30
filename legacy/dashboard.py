"""
dashboard.py -- minimal results dashboard for the Smart Agriculture project.

Reads the CSV results already persisted by the Scala pipeline (Kalman
robustness, ML metrics/feature importance, conformal coverage, decisions,
streaming fallback demo) and produces static plots + one summary HTML page.

Deliberately NOT a live web app / framework dashboard (Phase 9's explicit
allowance: "if a dashboard framework creates dependency problems, generate
static plots/HTML/CSV results instead") -- this reads results that were
already computed and saved by real Spark/Scala runs; it does not recompute
or fabricate anything itself.
"""
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
OUT_DIR = os.path.join(RESULTS_DIR, "dashboard")
os.makedirs(OUT_DIR, exist_ok=True)


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def plot_kalman_robustness():
    rows = read_csv(os.path.join(RESULTS_DIR, "kalman", "robustness_summary.csv"))
    scenarios = [r["scenario"] for r in rows]
    static_rmse = [float(r["static_rmse"]) for r in rows]
    adaptive_rmse = [float(r["adaptive_rmse"]) for r in rows]

    x = range(len(scenarios))
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar([i - width / 2 for i in x], static_rmse, width, label="Static Kalman")
    ax.bar([i + width / 2 for i in x], adaptive_rmse, width, label="Adaptive Kalman")
    ax.set_xticks(list(x))
    ax.set_xticklabels(scenarios, rotation=15, ha="right")
    ax.set_ylabel("RMSE vs. ground truth")
    ax.set_title("Static vs. Adaptive Reliability-Aware Kalman: RMSE by scenario\n(real Task 5/6 code; ground truth used only for evaluation)")
    ax.legend()
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "kalman_robustness.png")
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path, rows


def plot_feature_importance():
    rows = read_csv(os.path.join(RESULTS_DIR, "ml", "feature_importances.csv"))
    features = [r["feature"] for r in rows]
    importances = [float(r["importance"]) for r in rows]

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.barh(features[::-1], importances[::-1], color="#4C72B0")
    ax.set_xlabel("Model feature importance (RandomForest, not a causal measure)")
    ax.set_title("Top features -- crop-stress classifier\n(sandbox-scale verification run; see run_metadata.csv)")
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "feature_importance.png")
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path, rows


def plot_decisions():
    rows = read_csv(os.path.join(RESULTS_DIR, "decisions", "decisions.csv"))
    counts = {}
    for r in rows:
        counts[r["action"]] = counts.get(r["action"], 0) + 1

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.bar(list(counts.keys()), list(counts.values()), color=["#55A868", "#DD8452", "#C44E52"])
    ax.set_ylabel("Number of test samples")
    ax.set_title("Decision-engine action counts (test set)")
    fig.tight_layout()
    out_path = os.path.join(OUT_DIR, "decision_counts.png")
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path, counts


def main():
    kalman_plot, kalman_rows = plot_kalman_robustness()
    fi_plot, fi_rows = plot_feature_importance()
    decision_plot, decision_counts = plot_decisions()

    ml_metrics = {r["metric"]: r["value"] for r in read_csv(os.path.join(RESULTS_DIR, "ml", "metrics.csv"))}
    ml_meta = {r["key"]: r["value"] for r in read_csv(os.path.join(RESULTS_DIR, "ml", "run_metadata.csv"))}
    conformal = read_csv(os.path.join(RESULTS_DIR, "conformal", "coverage_report.csv"))[0]
    streaming = {r["metric"]: r["value"] for r in read_csv(os.path.join(RESULTS_DIR, "streaming", "fallback_demo_summary.csv"))}

    kalman_table_rows = "".join(
        "<tr><td>{}</td><td>{:.5f}</td><td>{:.5f}</td><td>{:+.5f}</td></tr>".format(
            r["scenario"], float(r["static_rmse"]), float(r["adaptive_rmse"]), float(r["delta"])
        )
        for r in kalman_rows
    )
    ml_table_rows = "".join("<tr><td>{}</td><td>{}</td></tr>".format(k, v) for k, v in ml_metrics.items())
    streaming_table_rows = "".join("<tr><td>{}</td><td>{}</td></tr>".format(k, v) for k, v in streaming.items())

    html = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Smart Agriculture -- Results Dashboard</title>
<style>
body {{ font-family: -apple-system, sans-serif; max-width: 900px; margin: 2em auto; padding: 0 1em; color: #222; }}
h1 {{ font-size: 1.5em; }} h2 {{ font-size: 1.15em; border-bottom: 1px solid #ddd; padding-bottom: .3em; margin-top: 2em; }}
table {{ border-collapse: collapse; width: 100%; margin: .5em 0; }}
td, th {{ border: 1px solid #ddd; padding: .4em .6em; text-align: left; font-size: .92em; }}
th {{ background: #f5f5f5; }}
img {{ max-width: 100%; border: 1px solid #ddd; margin: .5em 0; }}
.note {{ color: #666; font-size: .85em; }}
</style></head><body>
<h1>Smart Agriculture Big Data Project -- Results Dashboard</h1>
<p class="note">Generated from real, persisted Spark/Scala run outputs. Sandbox-scale verification (see notes) -- not full-dataset production numbers.</p>

<h2>1. Adaptive vs. Static Kalman Robustness</h2>
<img src="kalman_robustness.png" alt="Kalman robustness">
<table><tr><th>Scenario</th><th>Static RMSE</th><th>Adaptive RMSE</th><th>Delta</th></tr>
{kalman_rows}
</table>
<p class="note">Negative delta = adaptive Kalman has lower error (better). Adaptive costs a small amount on clean/missing data but substantially reduces error under outlier and bias corruption.</p>

<h2>2. Spark ML -- Crop Stress Prediction</h2>
<table>
<tr><th>Metric</th><th>Value</th></tr>
{ml_rows}
</table>
<p class="note">Run scope: {cells} cells, {train} train / {test} test rows, numTrees={nt}, maxDepth={md} (documented code default: numTrees={dnt}, maxDepth={dmd}). {scope_note}</p>
<img src="feature_importance.png" alt="Feature importance">

<h2>3. Conformal Prediction</h2>
<table><tr><th>alpha</th><th>qhat</th><th>Empirical coverage</th><th>Avg. set size</th><th>Ambiguous</th><th>Total</th></tr>
<tr><td>{alpha}</td><td>{qhat:.4f}</td><td>{cov:.4f}</td><td>{setsize:.4f}</td><td>{amb}</td><td>{tot}</td></tr>
</table>
<p class="note">Split conformal, finite-sample-corrected quantile. Target coverage = 1-alpha = {target_cov:.2f}. Marginal coverage guarantee under exchangeability -- see ConformalPrediction.scala for the exact, documented method.</p>

<h2>4. Decision Engine</h2>
<img src="decision_counts.png" alt="Decision counts">
<p class="note">Deterministic rule combining predicted probability and conformal ambiguity. Decision-support demonstration only, not an agronomic recommendation.</p>

<h2>5. Streaming (IoT -&gt; Kalman) Fallback Demonstration</h2>
<table>
{streaming_rows}
</table>
<p class="note">{mode}: no live Kafka broker was available in this environment; the same ObservableReading schema and the same StreamingKalmanPipeline.processBatch logic that a live Kafka micro-batch would use were exercised against a locally generated batch instead.</p>

</body></html>
""".format(
        kalman_rows=kalman_table_rows,
        ml_rows=ml_table_rows,
        cells=ml_meta.get("cells_in_subset"),
        train=ml_meta.get("train_rows"),
        test=ml_meta.get("test_rows"),
        nt=ml_meta.get("num_trees_used_this_run"),
        md=ml_meta.get("max_depth_used_this_run"),
        dnt=ml_meta.get("documented_default_num_trees"),
        dmd=ml_meta.get("documented_default_max_depth"),
        scope_note=ml_meta.get("sandbox_scope_note"),
        alpha=conformal["alpha"],
        qhat=float(conformal["qhat"]),
        cov=float(conformal["empirical_coverage"]),
        setsize=float(conformal["average_set_size"]),
        amb=conformal["ambiguous_count"],
        tot=conformal["total_count"],
        target_cov=1 - float(conformal["alpha"]),
        streaming_rows=streaming_table_rows,
        mode=streaming.get("mode"),
    )

    out_html = os.path.join(OUT_DIR, "index.html")
    with open(out_html, "w") as f:
        f.write(html)
    print("Dashboard written to " + out_html)
    print("Plots: " + kalman_plot + ", " + fi_plot + ", " + decision_plot)


if __name__ == "__main__":
    main()
