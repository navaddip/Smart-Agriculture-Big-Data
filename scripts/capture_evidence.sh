#!/bin/bash
# Captures text evidence of HDFS storage + YARN execution into docs/evidence/hdfs_yarn/.
# (Browser screenshots of the web UIs: see scripts/capture_screenshots.ps1.)
set -uo pipefail
export MSYS_NO_PATHCONV=1
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/docs/evidence/hdfs_yarn"; mkdir -p "$OUT"
B=/user/smart_agri
nn() { docker exec namenode bash -c "export HADOOP_ROOT_LOGGER=WARN,console; $1"; }

echo "== cluster report (5 DataNodes) ==";      nn "hdfs dfsadmin -report"                       > "$OUT/dfsadmin_report.txt"
grep -E "Live datanodes|^Name:|DFS Used%|Num of Blocks" "$OUT/dfsadmin_report.txt"
echo "== topology ==";                           nn "hdfs dfsadmin -printTopology"                > "$OUT/topology.txt"; cat "$OUT/topology.txt"
echo "== replication factor ==";                 nn "hdfs getconf -confKey dfs.replication"       > "$OUT/replication_config.txt"; cat "$OUT/replication_config.txt"
echo "== raw + processed zones ==";              nn "hdfs dfs -ls -R $B/raw $B/processed | grep -v '/_temporary'" > "$OUT/hdfs_ls_recursive.txt"
nn "hdfs dfs -du -h $B/raw/sentinel2 $B/raw/era5 $B/raw/soilgrids $B/processed $B/results $B/streaming" | tee "$OUT/hdfs_du.txt"
nn "hdfs dfs -count -q -h $B/raw $B/processed $B/results $B/streaming"                              | tee "$OUT/hdfs_count.txt"
echo "== fsck: block replication on raw + processed =="
nn "hdfs fsck $B/raw -files -blocks -locations"   > "$OUT/fsck_raw_blocks.txt"
nn "hdfs fsck $B/processed -files -blocks -locations" > "$OUT/fsck_processed_blocks.txt"
nn "hdfs fsck $B"                                   > "$OUT/fsck_summary.txt"
grep -E "Total size|Total files|Total blocks|Minimally replicated|Under-replicated|Default replication|Average block replication|Number of data-nodes|Status" "$OUT/fsck_summary.txt"
echo "== one raw Sentinel-2 file: blocks and the 3 DataNodes holding each =="
f=$(nn "hdfs dfs -ls $B/raw/sentinel2" | awk '/\.zip$/{print $NF; exit}')
nn "hdfs fsck '$f' -files -blocks -locations" | grep -E "blk_|Total|repl=" | head -14 | tee "$OUT/fsck_one_sentinel_file.txt"
echo "== YARN applications (Spark + MapReduce jobs that ran on the cluster) =="
docker exec resourcemanager yarn application -list -appStates ALL 2>/dev/null | tee "$OUT/yarn_applications.txt" | head -40
docker exec resourcemanager yarn node -list -all 2>/dev/null > "$OUT/yarn_nodes.txt"
echo "== Kafka topics ==";  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe > "$OUT/kafka_topics.txt" 2>&1; cat "$OUT/kafka_topics.txt"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}" | tee "$OUT/docker_containers.txt"
echo "evidence written to docs/evidence/hdfs_yarn/"
