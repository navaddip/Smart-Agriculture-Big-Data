# Captures PNG screenshots of the cluster web UIs (evidence of HDFS storage and job execution)
# with headless Microsoft Edge, into docs/evidence/screenshots/.
#
#   powershell -ExecutionPolicy Bypass -File scripts\capture_screenshots.ps1
#
# Requires the cluster to be up (docker compose up -d). The Spark History Server is started
# inside spark-client if it is not already running.

$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$out = Join-Path $root "docs\evidence\screenshots"
New-Item -ItemType Directory -Force $out | Out-Null
$edge = @("C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
          "C:\Program Files\Microsoft\Edge\Application\msedge.exe") | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $edge) { Write-Error "Microsoft Edge not found"; exit 1 }

docker exec spark-client bash -c "pgrep -f HistoryServer >/dev/null || /opt/spark/sbin/start-history-server.sh" | Out-Null
Start-Sleep -Seconds 20

$base = "/user/smart_agri"
$pages = [ordered]@{
  "01_namenode_overview"        = "http://localhost:9870/dfshealth.html#tab-overview"
  "02_namenode_5_datanodes"     = "http://localhost:9870/dfshealth.html#tab-datanode"
  "03_hdfs_raw_zone"            = "http://localhost:9870/explorer.html#$base/raw"
  "04_hdfs_raw_sentinel2_48GB"  = "http://localhost:9870/explorer.html#$base/raw/sentinel2"
  "05_hdfs_raw_era5"            = "http://localhost:9870/explorer.html#$base/raw/era5"
  "06_hdfs_raw_soilgrids"       = "http://localhost:9870/explorer.html#$base/raw/soilgrids"
  "07_hdfs_processed_zone"      = "http://localhost:9870/explorer.html#$base/processed"
  "08_hdfs_processed_sentinel2" = "http://localhost:9870/explorer.html#$base/processed/sentinel2_ndvi.parquet"
  "09_hdfs_processed_reports"   = "http://localhost:9870/explorer.html#$base/processed/_reports"
  "10_hdfs_results"             = "http://localhost:9870/explorer.html#$base/results"
  "11_hdfs_streaming"           = "http://localhost:9870/explorer.html#$base/streaming"
  "12_yarn_applications"        = "http://localhost:8088/cluster/apps"
  "13_yarn_nodes"               = "http://localhost:8088/cluster/nodes"
  "14_yarn_scheduler"           = "http://localhost:8088/cluster/scheduler"
  "15_spark_history"            = "http://localhost:18080/?showIncomplete=false"
}
foreach ($name in $pages.Keys) {
  $file = Join-Path $out "$name.png"
  & $edge --headless=new --disable-gpu --hide-scrollbars --window-size=1500,1100 `
          --virtual-time-budget=10000 --screenshot="$file" $pages[$name] 2>$null
  for ($i = 0; $i -lt 20 -and -not (Test-Path $file); $i++) { Start-Sleep -Milliseconds 500 }
  if (Test-Path $file) { "saved  $name.png" } else { "FAILED $name" }
}
