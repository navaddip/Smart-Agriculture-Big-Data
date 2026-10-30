#!/bin/bash
# Submits one class of the project jar to the YARN cluster. Runs inside the spark-client container:
#
#   docker exec spark-client bash /scripts/submit.sh <main-class> [--conf k=v ...] [-- job args...]
#
# Everything before "--" is passed to spark-submit, everything after it to the job.
# Defaults (master=yarn, executors, event log on HDFS) come from cluster/spark-conf/spark-defaults.conf.
set -euo pipefail
CLASS="$1"; shift
SUBMIT_OPTS=(); JOB_ARGS=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--" ]; then shift; JOB_ARGS=("$@"); break; fi
  SUBMIT_OPTS+=("$1"); shift
done
exec /opt/spark/bin/spark-submit \
  --class "$CLASS" \
  --conf spark.ui.showConsoleProgress=false \
  --driver-java-options "-Dlog4j2.level=warn" \
  "${SUBMIT_OPTS[@]+"${SUBMIT_OPTS[@]}"}" \
  /jars/smart-agri-ingestion-assembly-1.0.jar \
  "${JOB_ARGS[@]+"${JOB_ARGS[@]}"}"
