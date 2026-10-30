#!/bin/bash
# =============================================================================
# Smart Agriculture Big Data pipeline -- one command, start to end, on the cluster.
#
#   docker compose up -d            # once: HDFS (5 DataNodes, rf=3) + YARN + Spark client + Kafka
#   bash scripts/run_pipeline.sh    # from the project root (Git Bash / WSL / Linux / macOS)
#
# Every stage runs on the Hadoop cluster (Spark-on-YARN, MapReduce-on-YARN, HDFS),
# logs to docs/evidence/NN_<stage>.log and reports SUCCESS / SKIPPED / FAILED.
# Stages whose HDFS output already exists are SKIPPED (resumable). FORCE=1 re-runs all.
# ONLY="7 8" runs just those stage numbers.
# =============================================================================
set -uo pipefail
export MSYS_NO_PATHCONV=1            # Git Bash: do not rewrite container paths

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EVID="$ROOT/docs/evidence"; mkdir -p "$EVID" "$ROOT/results"
HB=hdfs://namenode:9000/user/smart_agri
JAR=/jars/smart-agri-ingestion-assembly-1.0.jar
FORCE="${FORCE:-0}"; ONLY="${ONLY:-}"
declare -a SUMMARY

hdfs_exists() { docker exec namenode hdfs dfs -test -e "$1" 2>/dev/null; }
submit()      { docker exec spark-client bash /scripts/submit.sh "$@"; }

stage() {  # stage <nn> <name> <skip-if-this-hdfs-path-exists|-> <command...>
  local nn="$1" name="$2" marker="$3"; shift 3
  local log="$EVID/${nn}_${name}.log"
  if [ -n "$ONLY" ] && [[ " $ONLY " != *" $((10#$nn)) "* ]]; then return; fi
  if [ "$FORCE" != "1" ] && [ "$marker" != "-" ] && hdfs_exists "$marker"; then
    echo "[$nn] $name: SKIPPED (output exists: $marker)"; SUMMARY+=("$nn $name SKIPPED"); return
  fi
  echo "[$nn] $name: running ... (log: docs/evidence/${nn}_${name}.log)"
  local t0=$SECONDS
  if "$@" > "$log" 2>&1; then
    echo "[$nn] $name: SUCCESS ($((SECONDS - t0)) s)"; SUMMARY+=("$nn $name SUCCESS $((SECONDS - t0))s")
  else
    echo "[$nn] $name: FAILED -- last lines:"; tail -15 "$log" | sed 's/^/      /'
    SUMMARY+=("$nn $name FAILED"); return 1
  fi
}

# ---- 00 cluster health ------------------------------------------------------
cluster_check() {
  docker exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity|DFS Used:" | head -3
  local live; live=$(docker exec namenode hdfs dfsadmin -report 2>/dev/null | grep -oP "Live datanodes \(\K[0-9]+")
  echo "live DataNodes: $live"; [ "${live:-0}" -ge 3 ] || { echo "need >= 3 live DataNodes for replication 3"; return 1; }
  docker exec resourcemanager yarn node -list 2>/dev/null
  docker exec namenode hdfs getconf -confKey dfs.replication
  # one-time: Spark's jars staged in HDFS so YARN executors do not re-upload them per job
  if ! hdfs_exists /spark/spark-jars.zip; then
    docker exec namenode hdfs dfs -mkdir -p /spark /spark-logs
    docker exec spark-client python3 -c "import zipfile,glob,os; os.chdir('/opt/spark/jars'); z=zipfile.ZipFile('/results/.spark-jars.zip','w'); [z.write(f) for f in sorted(glob.glob('*.jar'))]; z.close()"
    docker cp "$ROOT/results/.spark-jars.zip" namenode:/tmp/spark-jars.zip && rm -f "$ROOT/results/.spark-jars.zip"
    docker exec namenode hdfs dfs -put -f /tmp/spark-jars.zip /spark/spark-jars.zip && docker exec namenode rm /tmp/spark-jars.zip
  fi
  docker exec namenode hdfs dfs -mkdir -p /spark-logs
}
stage 00 cluster_check - cluster_check || exit 1

# ---- 01 raw data -> HDFS ----------------------------------------------------
stage 01 hdfs_raw_load - docker exec namenode bash /scripts/hdfs_load_raw.sh || exit 1

# ---- 02-04 Scala/Spark preprocessing (raw HDFS -> processed HDFS) ------------
stage 02 preprocess_sentinel2 "$HB/processed/sentinel2_ndvi.parquet/_SUCCESS" \
  submit smartagri.preprocess.Sentinel2Preprocess --conf spark.executor.cores=1 --conf spark.executor.memory=1536m || exit 1
stage 03 preprocess_era5 "$HB/processed/era5_weather.parquet/_SUCCESS" \
  submit smartagri.preprocess.Era5Preprocess || exit 1
stage 04 preprocess_soilgrids "$HB/processed/soilgrids.parquet/_SUCCESS" \
  submit smartagri.preprocess.SoilGridsPreprocess || exit 1

# ---- 05 canonical multimodal dataset -----------------------------------------
stage 05 canonical_dataset "$HB/processed/canonical_historical.parquet/_SUCCESS" \
  submit smartagri.canonical.CanonicalHistoricalDatasetBuilder || exit 1

# ---- 06 classic Hadoop MapReduce on YARN (NDVI per tile per month) -----------
mapreduce() {
  submit smartagri.mapreduce.NdviFlatInputPreparation || return 1
  docker exec namenode hdfs dfs -rm -r -f -skipTrash "$HB/processed/mapreduce/ndvi_tile_monthly" >/dev/null 2>&1
  docker exec resourcemanager hadoop jar "$JAR" smartagri.mapreduce.NdviTileMonthDriver \
    "$HB/processed/mapreduce/ndvi_flat_input" "$HB/processed/mapreduce/ndvi_tile_monthly" || return 1
  echo "--- MapReduce output (tile, month -> NDVI statistics) ---"
  docker exec namenode hdfs dfs -cat "$HB/processed/mapreduce/ndvi_tile_monthly/part-r-*" | tee "$ROOT/results/mapreduce_ndvi_tile_monthly.tsv"
}
stage 06 mapreduce_ndvi_tile_month "$HB/processed/mapreduce/ndvi_tile_monthly/_SUCCESS" mapreduce || exit 1

# ---- 07 leakage-safe features + future-stress target ---------------------------
stage 07 feature_pipeline "$HB/processed/ml_features/_SUCCESS" \
  submit smartagri.features.FeaturePipelineBuilder || exit 1

# ---- 08 Kalman robustness + ablation (Novelties 1 & 2) -------------------------
stage 08 kalman_robustness "$HB/results/kalman/robustness_summary/_SUCCESS" \
  submit smartagri.kalman.RobustnessExperiment -- --series 2000 || exit 1

# ---- 09 ML + baselines + conformal + decisions (Novelties 3 & 5) ----------------
stage 09 ml_train_evaluate "$HB/results/decisions/decisions.parquet/_SUCCESS" \
  submit smartagri.ml.TrainEvaluateJob || exit 1

# ---- 10 leakage audit (Novelty 4) --------------------------------------------
stage 10 leakage_audit - docker exec spark-client bash -c \
  "java -cp '$JAR:/opt/spark/jars/*' smartagri.audit.LeakageAudit /src/main/scala /results/leakage_audit/leakage_audit_report.csv" || exit 1

# ---- 11 IoT -> Kafka -> Structured Streaming -> alerts ----------------------------
streaming() {
  for t in iot_sensor_readings:6 crop_alerts:3; do
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "${t%%:*}" --partitions "${t##*:}" --replication-factor 1
  done
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
  docker exec namenode hdfs dfs -rm -r -f -skipTrash "$HB/streaming/checkpoints/crop_health" "$HB/streaming/alerts" "$HB/streaming/iot_filtered" >/dev/null 2>&1
  # the stream processor runs on YARN; the simulator starts once it is up
  docker exec spark-client bash /scripts/submit.sh smartagri.streaming.CropHealthStreamingJob \
    --conf spark.executor.instances=2 -- --seconds "${STREAM_SECONDS:-170}" &
  local spid=$!
  sleep 45
  docker exec spark-client java -cp "$JAR:/opt/spark/jars/*" smartagri.iot.IoTFieldSimulator \
    --bootstrap kafka:29092 --cells 200 --seconds "${SIM_SECONDS:-110}"
  wait $spid || return 1
  submit smartagri.iot.StreamingDetectionEvaluation
  echo "--- sample of alerts delivered on Kafka topic crop_alerts ---"
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic crop_alerts --from-beginning --max-messages 8 --timeout-ms 20000 | tee "$ROOT/results/streaming_alert_sample.jsonl"
  docker exec kafka /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 --topic crop_alerts
}
stage 11 streaming_kafka - streaming || exit 1

# ---- 12b dashboard (pure Scala, reads results/*.csv) ----------------------------
stage 13 dashboard - docker exec spark-client java -cp "$JAR" smartagri.report.DashboardReport /results

# ---- 12 evidence (HDFS / YARN state) --------------------------------------------
stage 12 hdfs_yarn_evidence - bash "$ROOT/scripts/capture_evidence.sh"

echo; echo "================ PIPELINE SUMMARY ================"
printf '  %s\n' "${SUMMARY[@]}"
