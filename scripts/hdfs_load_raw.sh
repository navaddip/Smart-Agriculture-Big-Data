#!/bin/bash
# Loads the raw, as-downloaded datasets into HDFS (replication factor 3).
# Runs INSIDE the namenode container, where ./data is mounted read-only at /data:
#
#   docker exec namenode bash /scripts/hdfs_load_raw.sh
#
# Raw zones are immutable copies of the downloads -- no filtering, renaming
# or de-duplication happens here. All cleaning is done later by the Scala
# preprocessing jobs, which read from these paths.
# Re-runnable: a file already in HDFS with the same byte size is skipped.

set -uo pipefail
export HADOOP_ROOT_LOGGER=WARN,console
BASE=/user/smart_agri
hdfs dfs -mkdir -p $BASE/raw/sentinel2 $BASE/raw/era5 $BASE/raw/soilgrids \
                   $BASE/processed $BASE/results $BASE/streaming /spark-logs /spark

put_dir() {  # $1 = local dir, $2 = HDFS dir, $3 = filename glob
  local src="$1" dst="$2" pat="$3" n=0 total
  total=$(ls "$src"/$pat 2>/dev/null | wc -l)
  for f in "$src"/$pat; do
    [ -e "$f" ] || continue
    n=$((n+1))
    local name size remote
    name=$(basename "$f"); size=$(stat -c %s "$f")
    remote=$(hdfs dfs -stat %b "$dst/$name" 2>/dev/null || echo -1)
    if [ "$remote" = "$size" ]; then
      echo "[$n/$total] SKIP  $name (already in HDFS)"
      continue
    fi
    echo "[$n/$total] PUT   $name ($((size/1024/1024)) MB)"
    # stream via stdin: Hadoop mis-parses local paths that contain spaces
    hdfs dfs -put -f - "$dst/$name" < "$f" || { echo "FAILED: $name"; exit 1; }
  done
}

date
put_dir /data/soilgrids $BASE/raw/soilgrids "*.tif"
put_dir /data/era5      $BASE/raw/era5      "*"
put_dir /data/sentinel2 $BASE/raw/sentinel2 "*.zip"
date

hdfs dfs -du -s -h $BASE/raw/sentinel2 $BASE/raw/era5 $BASE/raw/soilgrids
hdfs dfs -count -h $BASE/raw
echo "RAW LOAD COMPLETE"
