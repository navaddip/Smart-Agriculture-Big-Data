package smartagri.audit

import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.Try

/**
 * Novelty 4: automated, self-verifying leakage audit.
 *
 *   java -cp app.jar:<spark jars> smartagri.audit.LeakageAudit <scala-source-root> <report.csv>
 *
 * Three kinds of evidence, all computed -- nothing is asserted by hand:
 *
 *  STRUCTURAL (Java reflection on the compiled classes): e.g. the ObservableReading type that
 *  flows into Kafka/streaming physically has no health / ground-truth field, so no code *can*
 *  read one; the conformal calibrator only accepts calibration examples; the decision input has
 *  no label field.
 *
 *  SCOPE-AWARE SOURCE SCANNING: the same identifier is legitimate in one scope and a leak in
 *  another. `future_ndvi` MUST exist where the target is constructed (features/) and MUST NOT
 *  appear in model-consuming code (ml/, streaming/, decision/, conformal/, kalman/). Ground-truth
 *  identifiers (trueValue, corruptionType, sensor_health ...) are allowed only in the simulator
 *  and in explicitly-evaluation-only code. Comments are stripped before scanning.
 *
 *  POSITIVE CONTROLS: every detector is run once against an injected, deliberately-leaky input
 *  and must FIRE. A detector that cannot catch a planted violation fails the audit, so a clean
 *  report is evidence rather than an absence of evidence.
 *
 * Exits 1 if any check (including a positive control) fails.
 */
object LeakageAudit {

  final case class Check(id: Int, category: String, name: String, passed: Boolean, detail: String)

  val FutureIdentifiers: Seq[String] = Seq("future_ndvi", "future_anomaly", "future_date", "future_gap_days")
  val TruthIdentifiers: Seq[String] = Seq("trueValue", "true_value", "corruptionType", "corruption_type",
    "corruptionSeverity", "corruption_severity", "sensor_health", "sensorHealth", "GroundTruthReading")

  /** Where target-construction identifiers are legitimate. */
  val FutureAllowedFiles: Set[String] = Set("features/FeatureEngineering.scala", "features/FutureStressTarget.scala",
    "features/FeatureSplit.scala", "features/FeaturePipelineBuilder.scala", "features/FeatureQualityReport.scala",
    "audit/LeakageAudit.scala")
  /** Model-consuming scopes where they are forbidden. */
  val FutureForbiddenDirs: Seq[String] = Seq("ml/", "streaming/", "decision/", "conformal/", "kalman/", "integration/")

  /** Ground truth may exist only in the simulator and in evaluation-only code (scored, never filtered on). */
  val TruthAllowedFiles: Set[String] = Set("iot/IotSchema.scala", "iot/IotSimulatorPipeline.scala",
    "iot/SensorSeriesGenerator.scala", "iot/IotSimulatorConfig.scala", "kalman/RobustnessExperiment.scala",
    "audit/LeakageAudit.scala", "model/IotSensorReading.scala", "features/FeaturePipelineBuilder.scala")
  val TruthForbiddenDirs: Seq[String] = Seq("ml/", "streaming/", "decision/", "conformal/", "integration/",
    "kafka/", "features/", "kalman/")

  // ------------------------------------------------------------------ source helpers

  /** Removes // and /* */ comments while keeping string literals (column names live in strings). */
  def stripComments(src: String): String = {
    val sb = new StringBuilder; var i = 0; var inStr = false
    while (i < src.length) {
      val ch = src(i)
      if (inStr) { sb += ch; if (ch == '\\' && i + 1 < src.length) { sb += src(i + 1); i += 1 } else if (ch == '"') inStr = false; i += 1 }
      else if (ch == '"') { inStr = true; sb += ch; i += 1 }
      else if (src.startsWith("//", i)) { while (i < src.length && src(i) != '\n') i += 1 }
      else if (src.startsWith("/*", i)) { val e = src.indexOf("*/", i + 2); i = if (e < 0) src.length else e + 2 }
      else { sb += ch; i += 1 }
    }
    sb.toString
  }

  def loadSources(root: File): Map[String, String] = {
    def walk(f: File): Seq[File] = if (f.isDirectory) Option(f.listFiles).toSeq.flatten.flatMap(walk) else Seq(f)
    val base = new File(root, "smartagri")
    walk(base).filter(_.getName.endsWith(".scala")).map { f =>
      val rel = base.toURI.relativize(f.toURI).getPath
      val s = Source.fromFile(f, "UTF-8"); try rel -> stripComments(s.mkString) finally s.close()
    }.toMap
  }

  /** Files in `forbiddenDirs` (and not allow-listed) that mention any of `ids`. */
  def scopeViolations(sources: Map[String, String], ids: Seq[String], forbiddenDirs: Seq[String],
                      allowed: Set[String]): Seq[String] =
    sources.toSeq.sortBy(_._1).flatMap { case (path, code) =>
      if (allowed.contains(path) || !forbiddenDirs.exists(path.startsWith)) Nil
      else ids.filter(id => ("\\b" + java.util.regex.Pattern.quote(id) + "\\b").r.findFirstIn(code).isDefined)
        .map(id => s"$path uses '$id'")
    }

  /** Every rowsBetween(lo, hi) whose upper bound is not strictly in the past (hi must be <= -1). */
  def windowBoundViolations(sources: Map[String, String]): Seq[String] =
    sources.toSeq.flatMap { case (path, code) =>
      """rowsBetween\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)""".r.findAllMatchIn(code).flatMap { m =>
        val hi = m.group(2).trim
        val ok = Try(hi.toInt).toOption.exists(_ <= -1)
        if (ok) None else Some(s"$path: rowsBetween(${m.group(1)}, $hi) upper bound is not strictly prior")
      }
    }

  def fieldNames(cls: Class[_]): Set[String] = cls.getDeclaredFields.map(_.getName).filterNot(_.contains("$")).toSet

  // ------------------------------------------------------------------ the audit

  def run(allSources: Map[String, String]): Seq[Check] = {
    // The auditor's own file holds the planted positive-control strings; it is not pipeline code.
    val sources = allSources.filterKeys(!_.startsWith("audit/")).toMap
    val checks = scala.collection.mutable.ArrayBuffer.empty[Check]
    def add(cat: String, name: String, passed: Boolean, detail: String): Unit =
      checks += Check(checks.size + 1, cat, name, passed, detail)

    // --- structural (reflection)
    val obs = fieldNames(classOf[smartagri.iot.ObservableReading])
    val forbiddenObs = obs.filter(f => TruthIdentifiers.exists(_.equalsIgnoreCase(f)) || f.toLowerCase.contains("health"))
    add("structural", "ObservableReading (Kafka/stream type) has no health or ground-truth field", forbiddenObs.isEmpty,
      s"fields=${obs.toSeq.sorted.mkString("|")}")
    add("structural", "ObservableReading field set is exactly the observable schema",
      obs == Set("sensorId", "tile", "cellRow", "cellCol", "eventTime", "sensorType", "value"), obs.toSeq.sorted.mkString("|"))
    val gt = fieldNames(classOf[smartagri.iot.GroundTruthReading])
    add("structural", "ground truth lives in a separate type (GroundTruthReading)", gt.contains("trueValue") && !obs.contains("trueValue"),
      s"GroundTruthReading fields=${gt.toSeq.sorted.mkString("|")}")
    val di = fieldNames(classOf[smartagri.decision.DecisionEngine.DecisionInput])
    add("structural", "DecisionEngine.DecisionInput carries no label", !di.exists(f => f.toLowerCase.contains("label") || f.toLowerCase.contains("actual")),
      di.toSeq.sorted.mkString("|"))
    val calibrate = smartagri.conformal.ConformalPrediction.getClass.getMethods.filter(_.getName == "calibrate")
    add("structural", "ConformalPrediction.calibrate accepts calibration data only", calibrate.nonEmpty &&
      calibrate.forall(m => m.getGenericParameterTypes.head.getTypeName.contains("CalibrationExample")),
      calibrate.map(_.toGenericString).mkString("; "))

    // --- feature-set values
    val feats = smartagri.features.FeaturePipelineBuilder.approvedFeatureColumns()
    val futureInFeats = feats.filter(f => FutureIdentifiers.contains(f) || f == "future_stressed")
    add("feature-set", "no future / target column in the approved feature list", futureInFeats.isEmpty,
      s"${feats.size} features; offending=${futureInFeats.mkString("|")}")
    val truthInFeats = feats.filter(f => TruthIdentifiers.exists(_.equalsIgnoreCase(f)))
    add("feature-set", "no ground-truth / sensor-health column in the feature list", truthInFeats.isEmpty, truthInFeats.mkString("|"))
    val idInFeats = feats.intersect(smartagri.features.FeaturePipelineBuilder.IdentityColumns)
    add("feature-set", "identity columns (tile/cell/date) are not features", idInFeats.isEmpty, idInFeats.mkString("|"))
    add("feature-set", "feature list has no duplicates", feats.distinct.size == feats.size, s"${feats.size} listed")

    // --- scope-aware scanning
    val fut = scopeViolations(sources, FutureIdentifiers, FutureForbiddenDirs, FutureAllowedFiles)
    add("scope", "future_* identifiers absent from model-consuming code", fut.isEmpty, fut.mkString("; "))
    val futPresent = FutureIdentifiers.take(2).forall(id => sources.get("features/FeatureEngineering.scala").exists(_.contains(id)) ||
      sources.get("features/FutureStressTarget.scala").exists(_.contains(id)))
    add("scope", "future_* identifiers present where the target IS constructed (scanner sanity)", futPresent,
      "features/FeatureEngineering.scala, features/FutureStressTarget.scala")
    val truth = scopeViolations(sources, TruthIdentifiers, TruthForbiddenDirs, TruthAllowedFiles)
    add("scope", "ground-truth identifiers absent outside simulator / evaluation code", truth.isEmpty, truth.mkString("; "))

    // --- temporal discipline
    val wb = windowBoundViolations(sources)
    add("temporal", "every rowsBetween window ends strictly before the current row", wb.isEmpty, wb.mkString("; "))
    val fe = sources.getOrElse("features/FeatureEngineering.scala", "")
    val leadUses = sources.toSeq.filter { case (p, c) => """\blead\(""".r.findFirstIn(c).isDefined }.map(_._1)
    add("temporal", "lead() (look-ahead) used only for target construction", leadUses.forall(_ == "features/FeatureEngineering.scala") &&
      """(?s)def withFuturePairing.*?lead\(""".r.findFirstIn(fe).isDefined, leadUses.mkString("|"))
    val random = sources.toSeq.filter { case (p, c) => (p.startsWith("features/") || p.startsWith("ml/")) && c.contains("randomSplit") }.map(_._1)
    add("temporal", "no random row split in features/ or ml/ (chronological only)", random.isEmpty, random.mkString("|"))

    // --- train-only statistics
    val fpb = sources.getOrElse("features/FeaturePipelineBuilder.scala", "")
    val trainOnly = fpb.contains("anomalyDistribution(split.train)") && fpb.contains("computeImputationStatistics(trainWithTarget)") &&
      !"""anomalyDistribution\(split\.test|computeImputationStatistics\(test""".r.findFirstIn(fpb).isDefined
    add("train-only", "target distribution + imputation statistics computed on TRAIN only", trainOnly, "features/FeaturePipelineBuilder.scala")
    val tej = sources.getOrElse("ml/TrainEvaluateJob.scala", "")
    val calOnly = tej.contains("primaryCal.select") && """calibrate\(calScores""".r.findFirstIn(tej).isDefined &&
      !"""rfTest[^\n]*CalibrationExample""".r.findFirstIn(tej).isDefined
    add("train-only", "conformal qhat computed from the calibration split, not the test split", calOnly, "ml/TrainEvaluateJob.scala")

    val selOnly = tej.contains("m.transform(innerVal)") && !"""valAuc[^
]*(test|calib)""".r.findFirstIn(tej).isDefined
    add("train-only", "hyper-parameters and primary-model choice use a validation date inside the fit period only", selOnly, "ml/TrainEvaluateJob.scala")

    // --- positive controls: each detector must catch a planted violation
    val planted = Map(
      "ml/InjectedViolation.scala" -> """val x = df.select(col("future_ndvi"))""",
      "streaming/InjectedTruth.scala" -> """val y = reading.trueValue""",
      "features/InjectedWindow.scala" -> """val w = byCell.rowsBetween(-4, 0)""")
    val pcFuture = scopeViolations(planted, FutureIdentifiers, FutureForbiddenDirs, FutureAllowedFiles)
    add("positive-control", "planted future_ndvi in ml/ is detected", pcFuture.exists(_.contains("InjectedViolation")), pcFuture.mkString("; "))
    val pcTruth = scopeViolations(planted, TruthIdentifiers, TruthForbiddenDirs, TruthAllowedFiles)
    add("positive-control", "planted trueValue in streaming/ is detected", pcTruth.exists(_.contains("InjectedTruth")), pcTruth.mkString("; "))
    val pcWindow = windowBoundViolations(planted)
    add("positive-control", "planted rowsBetween(-4, 0) is detected", pcWindow.nonEmpty, pcWindow.mkString("; "))
    val leaky = fieldNames(classOf[LeakyReadingControl])
    add("positive-control", "reflection check flags a type that has a sensorHealth field",
      leaky.exists(f => f.toLowerCase.contains("health")), leaky.toSeq.sorted.mkString("|"))
    val commented = stripComments("// col(\"future_ndvi\")\n/* trueValue */ val ok = 1")
    add("positive-control", "comment stripping removes commented-out identifiers (no false positives)",
      !commented.contains("future_ndvi") && !commented.contains("trueValue"), commented.trim)

    checks.toSeq
  }

  def main(args: Array[String]): Unit = {
    val srcRoot = new File(if (args.length > 0) args(0) else "/src/main/scala")
    val out = new File(if (args.length > 1) args(1) else "/results/leakage_audit/leakage_audit_report.csv")
    val sources = loadSources(srcRoot)
    require(sources.nonEmpty, s"no Scala sources under $srcRoot/smartagri")
    val checks = run(sources)

    out.getParentFile.mkdirs()
    val pw = new PrintWriter(out, "UTF-8")
    try {
      pw.println("id,category,check,result,detail")
      checks.foreach(c => pw.println(s"""${c.id},${c.category},"${c.name}",${if (c.passed) "PASS" else "FAIL"},"${c.detail.replace("\"", "'")}""""))
    } finally pw.close()

    println(s"Scanned ${sources.size} source files under $srcRoot")
    checks.foreach(c => println(f"  [${if (c.passed) "PASS" else "FAIL"}] ${c.id}%2d ${c.category}%-17s ${c.name}"))
    val failed = checks.count(!_.passed)
    println(s"${checks.size - failed}/${checks.size} checks passed -> $out")
    if (failed > 0) sys.exit(1)
  }
}

/** Positive-control type for the reflection detector. Never used by pipeline code. */
final case class LeakyReadingControl(sensorId: String, value: Option[Double], sensorHealth: Double)
