package smartagri.report

import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.Try

/**
 * Builds results/dashboard/index.html -- the final presentation page -- from the CSV reports the
 * cluster jobs wrote under results/. Pure Scala (no plotting library): charts are inline SVG.
 *
 *   java -cp app.jar smartagri.report.DashboardReport /results
 */
object DashboardReport {

  type Table = (Seq[String], Seq[Seq[String]])

  // ------------------------------------------------------------------ CSV
  def parseLine(line: String, sep: Char): Seq[String] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    val cur = new StringBuilder; var inQ = false; var i = 0
    while (i < line.length) {
      val c = line(i)
      if (inQ) { if (c == '"' && i + 1 < line.length && line(i + 1) == '"') { cur += '"'; i += 1 } else if (c == '"') inQ = false else cur += c }
      else if (c == '"') inQ = true
      else if (c == sep) { out += cur.toString; cur.clear() }
      else cur += c
      i += 1
    }
    out += cur.toString
    out
  }

  def readTable(f: File, sep: Char = ','): Option[Table] = if (!f.exists()) None else Try {
    val s = Source.fromFile(f, "UTF-8")
    val lines = try s.getLines().filter(_.trim.nonEmpty).toList finally s.close()
    (parseLine(lines.head, sep), lines.tail.map(parseLine(_, sep)))
  }.toOption

  def col(t: Table, name: String): Seq[String] = { val i = t._1.indexOf(name); if (i < 0) Nil else t._2.map(r => if (i < r.size) r(i) else "") }
  def num(s: String): Double = Try(s.trim.toDouble).getOrElse(Double.NaN)

  // ------------------------------------------------------------------ HTML
  def esc(s: String): String = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

  def fmt(s: String): String = Try(s.toDouble).toOption match {
    case Some(d) if s.contains(".") || s.contains("E") =>
      if (math.abs(d) >= 1000) f"$d%,.0f" else if (math.abs(d) >= 1) f"$d%.3f" else f"$d%.4f"
    case Some(d) if math.abs(d) >= 10000 => f"${d.toLong}%,d"
    case _ => s
  }

  def html(t: Table, maxRows: Int = 60): String = {
    val head = t._1.map(h => s"<th>${esc(h)}</th>").mkString
    val rows = t._2.take(maxRows).map(r => "<tr>" + r.map(c => {
      val cls = if (c == "PASS" || c == "OK" || c == "KEPT" || c.startsWith("adaptive significantly better")) " class=ok"
        else if (c == "FAIL" || c == "FAILED" || c.contains("WORSE")) " class=bad" else ""
      s"<td$cls>${esc(fmt(c))}</td>"
    }).mkString + "</tr>").mkString
    val more = if (t._2.size > maxRows) s"<p class=note>showing $maxRows of ${t._2.size} rows (full CSV in results/)</p>" else ""
    s"<div class=tw><table><thead><tr>$head</tr></thead><tbody>$rows</tbody></table></div>$more"
  }

  /** Horizontal bar chart; `series` = (name, colourVar) with one value per label. */
  def bars(labels: Seq[String], series: Seq[(String, String, Seq[Double])], unit: String = "", width: Int = 760): String = {
    val barH = 14; val gap = 4; val groupGap = 12; val labelW = 210
    val maxV = series.flatMap(_._3).filterNot(_.isNaN).map(math.abs).foldLeft(1e-12)(math.max)
    val groupH = series.size * (barH + gap) + groupGap
    val h = labels.size * groupH + 30
    val plotW = width - labelW - 80
    val items = labels.zipWithIndex.map { case (l, i) =>
      val y0 = i * groupH + 20
      val lbl = s"""<text x="${labelW - 8}" y="${y0 + (series.size * (barH + gap)) / 2 + 4}" text-anchor="end" class="lbl">${esc(l)}</text>"""
      lbl + series.zipWithIndex.map { case ((_, color, vals), k) =>
        val v = vals.lift(i).getOrElse(Double.NaN)
        val w = if (v.isNaN) 0 else math.abs(v) / maxV * plotW
        val y = y0 + k * (barH + gap)
        s"""<rect x="$labelW" y="$y" width="${w.max(1)}" height="$barH" rx="2" fill="var($color)"/>""" +
          s"""<text x="${labelW + w + 6}" y="${y + barH - 3}" class="val">${if (v.isNaN) "n/a" else fmt(f"$v%.4f")}$unit</text>"""
      }.mkString
    }.mkString
    val legend = series.zipWithIndex.map { case ((n, color, _), k) =>
      s"""<rect x="${labelW + k * 170}" y="2" width="12" height="12" fill="var($color)"/><text x="${labelW + k * 170 + 18}" y="12" class="lbl">${esc(n)}</text>"""
    }.mkString
    s"""<svg viewBox="0 0 $width $h" width="100%" role="img">$legend$items</svg>"""
  }

  def section(id: String, title: String, intro: String, body: String): String =
    s"""<section id="$id"><h2>${esc(title)}</h2><p class=intro>$intro</p>$body</section>"""

  def main(args: Array[String]): Unit = {
    val root = new File(if (args.nonEmpty) args(0) else "/results")
    def t(p: String, sep: Char = ','): Option[Table] = readTable(new File(root, p), sep)
    val parts = scala.collection.mutable.ArrayBuffer.empty[String]
    val nav = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    def add(id: String, title: String, intro: String, body: String): Unit = { nav += ((id, title)); parts += section(id, title, intro, body) }

    // ---- scale / preprocessing
    val stats = t("preprocessing/sentinel2_product_stats.csv")
    val kpis = stats.map { s =>
      val zip = col(s, "zip_bytes").map(num).sum; val read = col(s, "bytes_read").map(num).sum
      val px = col(s, "pixels").map(num).sum; val cells = col(s, "cells_kept").map(num).sum
      Seq(f"${zip / 1e9}%.1f GB" -> "Sentinel-2 L2A zips decoded from HDFS", f"${read / 1e9}%.2f GB" -> "bytes actually read (3 bands + metadata)",
        f"${px / 1e9}%.2f bn" -> "pixels cloud-masked + NDVI-computed", f"${cells}%,.0f" -> "cell observations written",
        s"${s._2.size}" -> "products processed on YARN")
    }.getOrElse(Nil)
    val kpiHtml = kpis.map { case (v, l) => s"<div class=kpi><div class=kv>$v</div><div class=kl>$l</div></div>" }.mkString
    add("prep", "Preprocessing (Scala / Spark on YARN, HDFS -> HDFS)",
      "Raw data is read from the HDFS raw zone; cleaned, georeferenced, aggregated data is written back to the HDFS processed zone (replication factor 3).",
      s"<div class=kpis>$kpiHtml</div>" +
        Seq("sentinel2_summary" -> "Sentinel-2 per tile", "sentinel2_inventory" -> "Product inventory and de-duplication",
          "era5_summary" -> "ERA5 coverage", "era5_quality" -> "ERA5 cleaning report", "soilgrids_quality" -> "SoilGrids resampling / gap-filling",
          "soilgrids_texture_classes" -> "Derived USDA texture classes", "feature_pipeline_summary" -> "Feature pipeline (leakage-safe)")
          .flatMap { case (f, h) => t(s"preprocessing/$f.csv").map(x => s"<h3>$h</h3>" + html(x)) }.mkString +
        stats.map(s => "<h3>Per-product decode statistics</h3>" + html(s)).getOrElse(""))

    // ---- MapReduce (headerless TSV written by NdviTileMonthReducer)
    readTable(new File(root, "mapreduce_ndvi_tile_monthly.tsv"), '	').foreach { raw =>
      val header = Seq("tile", "year_month", "observation_count", "valid_count", "invalid_count", "missing_count",
        "mean_ndvi", "min_ndvi", "max_ndvi", "stddev_ndvi", "quality_rate")
      val rows = raw._1 +: raw._2
      val m: Table = (header, rows)
      add("mr", "Hadoop MapReduce: mean NDVI per tile per month",
        "Classic MapReduce job (Mapper/Reducer on YARN) over the processed NDVI exported to HDFS.",
        bars(rows.map(r => s"${r(0)} ${r(1)}"), Seq(("mean NDVI", "--c1", col(m, "mean_ndvi").map(num)))) + html(m))
    }

    // ---- Kalman
    t("kalman/robustness_summary.csv").foreach { k =>
      val sc = col(k, "scenario")
      add("kalman", "Novelty 1: adaptive reliability-driven Kalman vs static (Monte-Carlo on YARN)",
        "Mean RMSE against ground truth over independent simulated sensor series per scenario; 95% CI on the paired difference. Verdict is computed, not asserted.",
        bars(sc, Seq(("static R", "--c2", col(k, "static_rmse").map(num)), ("adaptive R_t", "--c1", col(k, "adaptive_rmse").map(num)))) + html(k) +
          t("kalman/ablation_study.csv").map(a => "<h3>Ablation: which reliability component helps?</h3>" + html(a, 80)).getOrElse("") +
          t("kalman/real_ndvi_injection.csv").map(r => "<h3>Injection on real Sentinel-2 NDVI series from HDFS (partition-local Kalman, Novelty 2)</h3>" + html(r)).getOrElse(""))
    }

    // ---- ML
    t("ml/baseline_comparison.csv").foreach { b =>
      val models = col(b, "model")
      add("ml", "Novelties 3 & 5: future-stress prediction vs baselines (same split, same features)",
        "Target = stress at the NEXT Sentinel observation (T+1) from information up to T; chronological split at 2024-04-07. All models see identical rows.",
        bars(models, Seq(("ROC-AUC", "--c1", col(b, "roc_auc").map(num)), ("balanced accuracy", "--c3", col(b, "balanced_accuracy").map(num)))) + html(b) +
          t("ml/class_balance.csv").map(x => "<h3>Class balance per partition</h3>" + html(x)).getOrElse("") +
          t("ml/feature_importances.csv").map { f =>
            "<h3>Random-forest feature importance</h3>" + bars(col(f, "feature").take(15), Seq(("importance", "--c1", col(f, "importance").map(num).take(15))))
          }.getOrElse("") + t("ml/run_metadata.csv").map(x => "<h3>Run metadata</h3>" + html(x)).getOrElse(""))
    }
    t("conformal/coverage_report.csv").foreach(c => add("conformal", "Split conformal prediction: coverage on the unseen test period",
      "qhat calibrated on the last training dates only; coverage measured on the test period.", html(c)))
    t("decisions/decision_summary.csv").foreach { d =>
      add("decisions", "Decision support", "Probability + conformal ambiguity -> action. observed_stress_rate shows how often each action's fields really became stressed.",
        bars(col(d, "action"), Seq(("fields", "--c1", col(d, "count").map(num)))) + html(d))
    }

    // ---- streaming
    t("streaming/detection_evaluation.csv").foreach { s =>
      val sample = Try { val src = Source.fromFile(new File(root, "streaming_alert_sample.jsonl"), "UTF-8"); try src.getLines().take(6).toList finally src.close() }.getOrElse(Nil)
      add("stream", "IoT -> Kafka -> Structured Streaming: live disease / water-stress alerts",
        "Per-sensor adaptive Kalman state kept across micro-batches; fields scored against the simulator's hidden conditions (evaluation only).",
        html(s) + (if (sample.nonEmpty) "<h3>Alerts delivered on Kafka topic crop_alerts</h3><pre>" + esc(sample.mkString("\n")) + "</pre>" else ""))
    }

    // ---- sensitivity experiments (results/experiments/<tag>/ml/baseline_comparison.csv)
    val expDirs = Option(new File(root, "experiments").listFiles).toSeq.flatten.filter(_.isDirectory).sortBy(_.getName)
    val expRows = (("main (min history 10)", t("ml/baseline_comparison.csv")) +: expDirs.map(d => (d.getName, t(s"experiments/${d.getName}/ml/baseline_comparison.csv"))))
      .flatMap { case (name, tbl) => tbl.toSeq.flatMap { b =>
        b._2.map(r => name +: Seq("model", "rows", "accuracy", "balanced_accuracy", "recall_stressed", "roc_auc").map(c => r(b._1.indexOf(c)))) } }
    if (expDirs.nonEmpty) add("experiments", "Sensitivity experiment: fewer prior observations required",
      "Exploratory only: the minimum-history requirement (approved value 10) is lowered to admit more training dates. These runs were compared on the test period, so they are NOT a tuned result -- the main results above stay the reference.",
      html((Seq("run", "model", "test_rows", "accuracy", "balanced_accuracy", "recall_stressed", "roc_auc"), expRows), 80) +
        expDirs.flatMap(d => t(s"experiments/${d.getName}/ml/class_balance.csv").map(x => s"<h3>Class balance, ${esc(d.getName)}</h3>" + html(x))).mkString)

    t("leakage_audit/leakage_audit_report.csv").foreach { a =>
      val passed = col(a, "result").count(_ == "PASS")
      add("audit", s"Novelty 4: leakage audit -- $passed / ${a._2.size} checks passed", "Reflection + scope-aware source scanning + positive controls.", html(a))
    }

    val navHtml = nav.map { case (id, title) => s"""<a href="#$id">${esc(title.split(":").head)}</a>""" }.mkString
    val page =
      s"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Smart Agriculture Results</title><style>
:root{--bg:#fbfaf7;--fg:#1d1f1c;--mut:#5d625a;--card:#fff;--line:#e3e1da;--c1:#2f7d4f;--c2:#b8b3a6;--c3:#4f6fa8;--ok:#1f7a45;--bad:#b3261e}
@media (prefers-color-scheme:dark){:root{--bg:#141613;--fg:#e8e6df;--mut:#a4a89f;--card:#1c1f1b;--line:#30342e;--c1:#5fbf85;--c2:#6b6e66;--c3:#86a3d9;--ok:#6fd49a;--bad:#ff8a80}}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
header{padding:28px 16px 8px;max-width:1100px;margin:auto}h1{margin:0 0 6px;font-size:26px}
nav{position:sticky;top:0;background:var(--bg);border-bottom:1px solid var(--line);padding:8px 16px;display:flex;gap:14px;flex-wrap:wrap;z-index:2}
nav a{color:var(--mut);text-decoration:none;font-size:13px}nav a:hover{color:var(--fg)}
main{max-width:1100px;margin:auto;padding:0 16px 60px}section{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:18px 20px;margin:18px 0}
h2{font-size:19px;margin:0 0 4px}h3{font-size:15px;margin:18px 0 6px}.intro,.note{color:var(--mut);margin:0 0 12px}.note{font-size:12px}
.tw{overflow-x:auto}table{border-collapse:collapse;font-size:12.5px;width:100%}th,td{border-bottom:1px solid var(--line);padding:5px 8px;text-align:left;white-space:nowrap}
th{color:var(--mut);font-weight:600}td.ok{color:var(--ok);font-weight:600}td.bad{color:var(--bad);font-weight:600}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:8px 0 12px}.kpi{border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.kv{font-size:22px;font-weight:650}.kl{color:var(--mut);font-size:12.5px}svg .lbl{font-size:11.5px;fill:var(--fg)}svg .val{font-size:10.5px;fill:var(--mut)}
pre{background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:10px;overflow-x:auto;font-size:11.5px}
</style></head><body><header><h1>Smart Agriculture Big Data -- Results</h1>
<p class=intro>Every number on this page was produced by the Scala/Spark/MapReduce/Kafka jobs on the Hadoop cluster (5 DataNodes, replication 3) and read from results/*.csv.</p></header>
<nav>$navHtml</nav><main>${parts.mkString}</main></body></html>"""

    val out = new File(root, "dashboard/index.html"); out.getParentFile.mkdirs()
    val pw = new PrintWriter(out, "UTF-8"); try pw.print(page) finally pw.close()
    println(s"Dashboard written to $out (${parts.size} sections)")
  }
}
