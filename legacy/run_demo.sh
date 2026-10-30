#!/bin/bash
# scripts/run_demo.sh
#
# ONE master entry point for the Smart Agriculture Big Data project demo.
#
# RESUMABLE / OUTPUT-AWARE: each expensive stage checks whether its
# results already exist and are non-empty before recomputing. A second
# run of this script (or a run after a partial failure) does not redo
# expensive work unnecessarily -- it reports SKIPPED for anything already
# done and only executes what's actually missing.
#
# Every stage reports exactly one of: SUCCESS / SKIPPED / BLOCKED / FAILED.
#
# Stages:
#   1. Environment/data validation.
#   2. Compile all Scala sources (Tasks 1-12) plus the three demo drivers.
#   3. Adaptive-vs-static Kalman robustness demonstration (Task 5/6).
#   4. IoT -> [Kafka or local fallback] -> Streaming -> Kalman demonstration
#      (Task 6/7/8/9) -- Kafka liveness is detected and reported; if no
#      broker is reachable, the SAME ObservableReading schema and SAME
#      StreamingKalmanPipeline logic run against a local batch instead
#      (never a fake alternative implementation).
#   5. Spark ML training/evaluation, conformal calibration, decision engine
#      (Tasks 10/11/12) on a bounded real-data cell subset.
#   6. Dashboard generation (Python), from whatever results exist.
#   7. Final summary.
#
# NOTE ON SCOPE: this sandbox has exactly 1 CPU core, and the full
# 398,027-row canonical+feature pipeline does not complete within a
# practical single run here. Stage 5 uses a small, deterministic,
# real-data CELL SUBSET (CELL_SUBSET_SIZE, auto-generated on first run) --
# same real input files, same unmodified Task 1-9 code, fewer cells.
# Every metric this stage's outputs report is labeled
# verification-scale / reduced-scale, never definitive full-dataset
# performance.
#
# Exits with a clear, specific error if a genuinely required dependency
# is missing -- it does not silently skip validation.

set -uo pipefail  # NOT -e: one stage's failure must not abort status reporting for the rest

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCALA_SRC="$PROJECT_ROOT/scala-ingestion/src/main/scala"
DEMO_SRC="$PROJECT_ROOT/scripts/demo_src"
RESULTS_DIR="$PROJECT_ROOT/results"
export CELL_SUBSET_SIZE="${CELL_SUBSET_SIZE:-150}"

declare -A STAGE_STATUS

nonempty() { [ -s "$1" ]; }

echo "=== Smart Agriculture Big Data Project -- Master Demo (resumable) ==="
echo "Project root: $PROJECT_ROOT"
echo "Cell subset size for the ML/conformal step: $CELL_SUBSET_SIZE"

# ---- Stage 1: environment/data validation ----
echo ""
echo "--- [1/7] Environment/data validation ---"
STAGE1_OK=1
SCALAC_BIN="${SCALAC_BIN:-scalac}"
if ! command -v "$SCALAC_BIN" >/dev/null 2>&1; then
  echo "BLOCKED: Scala compiler ('$SCALAC_BIN') not found. Set SCALAC_BIN to its path."
  STAGE1_OK=0
fi
SPARK_JARS_DIR="$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))" 2>/dev/null || true)"
if [ -z "$SPARK_JARS_DIR" ] || [ ! -d "$SPARK_JARS_DIR" ]; then
  echo "BLOCKED: Spark jars not found (expected via 'pip show pyspark')."
  STAGE1_OK=0
else
  SPARK_CP="$(ls "$SPARK_JARS_DIR"/*.jar | tr '\n' ':')"
fi
for f in processed/sentinel2_ndvi.parquet processed/era5_weather.parquet processed/soilgrids.parquet; do
  if [ ! -e "$PROJECT_ROOT/$f" ]; then
    echo "BLOCKED: required raw input '$f' not found under $PROJECT_ROOT."
    STAGE1_OK=0
  fi
done
mkdir -p "$RESULTS_DIR"/ml "$RESULTS_DIR"/kalman "$RESULTS_DIR"/conformal "$RESULTS_DIR"/decisions "$RESULTS_DIR"/streaming "$RESULTS_DIR"/dashboard
if [ "$STAGE1_OK" = "1" ]; then
  echo "SUCCESS: environment/data validation passed."
  STAGE_STATUS[1]="SUCCESS"
else
  echo "FAILED: cannot proceed -- see BLOCKED lines above."
  STAGE_STATUS[1]="FAILED"
  echo ""
  echo "=== Aborting: stage 1 failed. ==="
  exit 1
fi

# ---- Stage 2: compile ----
echo ""
echo "--- [2/7] Compiling all Scala sources ---"
BUILD_DIR="$PROJECT_ROOT/.demo_build"
if [ -d "$BUILD_DIR" ] && [ -n "$(find "$BUILD_DIR" -name '*.class' -print -quit 2>/dev/null)" ] && \
   [ -z "$(find "$SCALA_SRC" "$DEMO_SRC" -name '*.scala' -newer "$BUILD_DIR" -print -quit 2>/dev/null)" ]; then
  echo "SKIPPED: $BUILD_DIR already contains compiled classes newer than all sources."
  STAGE_STATUS[2]="SKIPPED"
else
  rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
  if "$SCALAC_BIN" -classpath "$SPARK_CP" -d "$BUILD_DIR" \
      "$SCALA_SRC"/smartagri/spatial/GridCell.scala \
      "$SCALA_SRC"/smartagri/time/CanonicalDate.scala \
      "$SCALA_SRC"/smartagri/config/AppConfig.scala \
      "$SCALA_SRC"/smartagri/model/*.scala \
      "$SCALA_SRC"/smartagri/validation/SchemaValidation.scala \
      "$SCALA_SRC"/smartagri/mapreduce/*.scala \
      "$SCALA_SRC"/smartagri/canonical/*.scala \
      "$SCALA_SRC"/smartagri/features/*.scala \
      "$SCALA_SRC"/smartagri/kalman/*.scala \
      "$SCALA_SRC"/smartagri/iot/*.scala \
      "$SCALA_SRC"/smartagri/kafka/KafkaConfig.scala \
      "$SCALA_SRC"/smartagri/kafka/KafkaSerialization.scala \
      "$SCALA_SRC"/smartagri/streaming/*.scala \
      "$SCALA_SRC"/smartagri/integration/*.scala \
      "$SCALA_SRC"/smartagri/ml/*.scala \
      "$SCALA_SRC"/smartagri/conformal/*.scala \
      "$SCALA_SRC"/smartagri/decision/*.scala \
    && "$SCALAC_BIN" -classpath "$SPARK_CP:$BUILD_DIR" -d "$BUILD_DIR" \
      "$DEMO_SRC"/RobustnessDemo.scala "$DEMO_SRC"/StreamingFallbackDemo.scala "$DEMO_SRC"/ConformalRun.scala
  then
    echo "SUCCESS: core library and demo drivers compiled."
    STAGE_STATUS[2]="SUCCESS"
  else
    echo "FAILED: compilation error -- see output above."
    STAGE_STATUS[2]="FAILED"
    echo ""
    echo "=== Aborting: stage 2 failed. ==="
    exit 1
  fi
fi
# NOTE: smartagri/kafka/IoTKafkaProducer.scala and IoTKafkaConsumer.scala require the real
# org.apache.kafka:kafka-clients jar (declared in build.sbt), not fetchable in this sandbox --
# intentionally excluded above. `sbt compile` with real internet access compiles the whole project.

JVM_OPTS=(--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  -Xmx3g)

# ---- Stage 3: Kalman robustness (skip if valid output exists) ----
echo ""
echo "--- [3/7] Adaptive vs. Static Kalman robustness demonstration ---"
KALMAN_OUT="$RESULTS_DIR/kalman/robustness_summary.csv"
if nonempty "$KALMAN_OUT"; then
  echo "SKIPPED: valid output already exists at $KALMAN_OUT ($(wc -l < "$KALMAN_OUT") lines)."
  STAGE_STATUS[3]="SKIPPED"
else
  if java "${JVM_OPTS[@]}" -cp "$SPARK_CP:$BUILD_DIR" RobustnessDemo; then
    if nonempty "$KALMAN_OUT"; then echo "SUCCESS."; STAGE_STATUS[3]="SUCCESS"; else echo "FAILED: ran but produced no output."; STAGE_STATUS[3]="FAILED"; fi
  else
    echo "FAILED: RobustnessDemo exited with an error."
    STAGE_STATUS[3]="FAILED"
  fi
fi

# ---- Stage 4: streaming fallback (skip if valid output exists) ----
echo ""
echo "--- [4/7] IoT simulator -> streaming -> Kalman demonstration ---"
if command -v nc >/dev/null 2>&1 && nc -z -w2 localhost 9092 2>/dev/null; then
  echo "Kafka broker reachable at localhost:9092 -- live mode available (producer/consumer still require the kafka-clients jar on the runtime classpath)."
else
  echo "Kafka live mode unavailable; local streaming-compatible mode used."
fi
STREAM_OUT="$RESULTS_DIR/streaming/fallback_demo_summary.csv"
if nonempty "$STREAM_OUT"; then
  echo "SKIPPED: valid output already exists at $STREAM_OUT ($(wc -l < "$STREAM_OUT") lines)."
  STAGE_STATUS[4]="SKIPPED"
else
  if java "${JVM_OPTS[@]}" -cp "$SPARK_CP:$BUILD_DIR" StreamingFallbackDemo; then
    if nonempty "$STREAM_OUT"; then echo "SUCCESS."; STAGE_STATUS[4]="SUCCESS"; else echo "FAILED: ran but produced no output."; STAGE_STATUS[4]="FAILED"; fi
  else
    echo "FAILED: StreamingFallbackDemo exited with an error."
    STAGE_STATUS[4]="FAILED"
  fi
fi

# ---- Stage 5: ML + conformal + decision (skip if valid output exists) ----
echo ""
echo "--- [5/7] Spark ML training/evaluation, conformal calibration, decision engine ---"
CONFORMAL_OUT="$RESULTS_DIR/conformal/coverage_report.csv"
DECISIONS_OUT="$RESULTS_DIR/decisions/decisions.csv"
ML_METRICS_OUT="$RESULTS_DIR/ml/metrics.csv"
if nonempty "$CONFORMAL_OUT" && nonempty "$DECISIONS_OUT" && nonempty "$ML_METRICS_OUT"; then
  echo "SKIPPED: valid persisted output already exists (conformal, decisions, and ML metrics all present) -- not recomputing expensive ML training."
  echo "  $CONFORMAL_OUT ($(wc -l < "$CONFORMAL_OUT") lines)"
  echo "  $DECISIONS_OUT ($(wc -l < "$DECISIONS_OUT") lines)"
  echo "  $ML_METRICS_OUT ($(wc -l < "$ML_METRICS_OUT") lines) -- NOTE: verification-scale metrics (see run_metadata.csv); not regenerated here to avoid an unnecessary expensive retrain."
  STAGE_STATUS[5]="SKIPPED"
else
  echo "Valid output not fully present -- running ConformalRun (the expensive stage; may exceed a 1-core sandbox's time budget)."
  if java "${JVM_OPTS[@]}" -cp "$SPARK_CP:$BUILD_DIR" ConformalRun; then
    if nonempty "$CONFORMAL_OUT" && nonempty "$DECISIONS_OUT"; then echo "SUCCESS."; STAGE_STATUS[5]="SUCCESS"; else echo "FAILED: ran but produced incomplete output."; STAGE_STATUS[5]="FAILED"; fi
  else
    echo "FAILED or interrupted (e.g. exceeded time budget in a constrained sandbox). Re-run this script -- stages 3/4 will SKIP and this stage will retry."
    STAGE_STATUS[5]="FAILED"
  fi
fi

# ---- Stage 6: dashboard (always regenerate -- cheap, and should reflect latest results) ----
echo ""
echo "--- [6/7] Generating results dashboard ---"
if python3 "$PROJECT_ROOT/dashboard.py"; then
  if nonempty "$RESULTS_DIR/dashboard/index.html"; then
    echo "SUCCESS: dashboard written to $RESULTS_DIR/dashboard/index.html"
    STAGE_STATUS[6]="SUCCESS"
  else
    echo "FAILED: dashboard.py ran but produced no index.html."
    STAGE_STATUS[6]="FAILED"
  fi
else
  echo "FAILED: dashboard.py exited with an error (likely a missing upstream results file from a FAILED stage above)."
  STAGE_STATUS[6]="FAILED"
fi

# ---- Stage 7: final summary ----
echo ""
echo "--- [7/7] Final summary ---"
for i in 1 2 3 4 5 6; do
  echo "  Stage $i: ${STAGE_STATUS[$i]:-NOT_RUN}"
done
OVERALL="SUCCESS"
for i in 1 2 3 4 5 6; do
  if [ "${STAGE_STATUS[$i]:-}" = "FAILED" ]; then OVERALL="FAILED"; fi
done
echo ""
echo "=== Overall: $OVERALL. Results in $RESULTS_DIR, dashboard at $RESULTS_DIR/dashboard/index.html ==="
[ "$OVERALL" = "SUCCESS" ]
