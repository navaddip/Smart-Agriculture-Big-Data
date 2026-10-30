# scripts/run_demo.ps1
#
# Native Windows (PowerShell) equivalent of run_demo.sh -- same 7 stages,
# same resumable/output-aware skip-if-valid-output-exists behavior, same
# SUCCESS/SKIPPED/BLOCKED/FAILED per-stage reporting.
#
# Prerequisites (same as the bash version):
#   - A Scala compiler (scalac) on PATH, or pass -ScalacBin <path to scalac.bat>
#   - Python 3 with pyspark installed (pip install pyspark) and pandas/matplotlib
#     for dashboard.py (pip install pandas matplotlib)
#   - processed\sentinel2_ndvi.parquet, processed\era5_weather.parquet,
#     processed\soilgrids.parquet present under the project root
#
# Usage (from the project root, in PowerShell):
#   .\scripts\run_demo.ps1
#   .\scripts\run_demo.ps1 -ScalacBin "C:\scala\bin\scalac.bat" -CellSubsetSize 300

param(
    [string]$ScalacBin = "scalac",
    [int]$CellSubsetSize = 150
)

$ErrorActionPreference = "Continue"  # a single stage's failure must not abort status reporting for the rest

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ScalaSrc = Join-Path $ProjectRoot "scala-ingestion\src\main\scala"
$DemoSrc = Join-Path $ProjectRoot "scripts\demo_src"
$ResultsDir = Join-Path $ProjectRoot "results"
$env:CELL_SUBSET_SIZE = $CellSubsetSize

$StageStatus = @{}

function Test-NonEmpty($path) {
    return (Test-Path $path) -and ((Get-Item $path).Length -gt 0)
}

Write-Host "=== Smart Agriculture Big Data Project -- Master Demo (resumable, Windows) ==="
Write-Host "Project root: $ProjectRoot"
Write-Host "Cell subset size for the ML/conformal step: $CellSubsetSize"

# ---- Stage 1: environment/data validation ----
Write-Host "`n--- [1/7] Environment/data validation ---"
$Stage1Ok = $true
if (-not (Get-Command $ScalacBin -ErrorAction SilentlyContinue)) {
    Write-Host "BLOCKED: Scala compiler ('$ScalacBin') not found. Pass -ScalacBin <path>."
    $Stage1Ok = $false
}
$SparkJarsDir = python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))" 2>$null
if (-not $SparkJarsDir -or -not (Test-Path $SparkJarsDir)) {
    Write-Host "BLOCKED: Spark jars not found (expected via 'pip show pyspark'). Run: pip install pyspark"
    $Stage1Ok = $false
} else {
    $SparkJars = Get-ChildItem -Path $SparkJarsDir -Filter "*.jar" | ForEach-Object { $_.FullName }
    $SparkCp = $SparkJars -join ";"
}
foreach ($f in @("processed\sentinel2_ndvi.parquet", "processed\era5_weather.parquet", "processed\soilgrids.parquet")) {
    $full = Join-Path $ProjectRoot $f
    if (-not (Test-Path $full)) {
        Write-Host "BLOCKED: required raw input '$f' not found under $ProjectRoot."
        $Stage1Ok = $false
    }
}
New-Item -ItemType Directory -Force -Path "$ResultsDir\ml","$ResultsDir\kalman","$ResultsDir\conformal","$ResultsDir\decisions","$ResultsDir\streaming","$ResultsDir\dashboard" | Out-Null
if ($Stage1Ok) {
    Write-Host "SUCCESS: environment/data validation passed."
    $StageStatus[1] = "SUCCESS"
} else {
    Write-Host "FAILED: cannot proceed -- see BLOCKED lines above."
    $StageStatus[1] = "FAILED"
    Write-Host "`n=== Aborting: stage 1 failed. ==="
    exit 1
}

# ---- Stage 2: compile ----
Write-Host "`n--- [2/7] Compiling all Scala sources ---"
$BuildDir = Join-Path $ProjectRoot ".demo_build"
$needCompile = $true
if ((Test-Path $BuildDir) -and (Get-ChildItem -Path $BuildDir -Filter "*.class" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1)) {
    $newestSource = Get-ChildItem -Path $ScalaSrc,$DemoSrc -Filter "*.scala" -Recurse | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $buildDirTime = (Get-Item $BuildDir).LastWriteTime
    if ($newestSource.LastWriteTime -le $buildDirTime) { $needCompile = $false }
}
if (-not $needCompile) {
    Write-Host "SKIPPED: $BuildDir already contains compiled classes newer than all sources."
    $StageStatus[2] = "SKIPPED"
} else {
    Remove-Item -Recurse -Force $BuildDir -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
    $coreFiles = @(
        "$ScalaSrc\smartagri\spatial\GridCell.scala",
        "$ScalaSrc\smartagri\time\CanonicalDate.scala",
        "$ScalaSrc\smartagri\config\AppConfig.scala"
    ) + (Get-ChildItem "$ScalaSrc\smartagri\model\*.scala").FullName + @(
        "$ScalaSrc\smartagri\validation\SchemaValidation.scala"
    ) + (Get-ChildItem "$ScalaSrc\smartagri\mapreduce\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\canonical\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\features\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\kalman\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\iot\*.scala").FullName `
      + @("$ScalaSrc\smartagri\kafka\KafkaConfig.scala", "$ScalaSrc\smartagri\kafka\KafkaSerialization.scala") `
      + (Get-ChildItem "$ScalaSrc\smartagri\streaming\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\integration\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\ml\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\conformal\*.scala").FullName `
      + (Get-ChildItem "$ScalaSrc\smartagri\decision\*.scala").FullName

    & $ScalacBin -classpath $SparkCp -d $BuildDir @coreFiles
    $coreOk = $LASTEXITCODE -eq 0
    $demoOk = $false
    if ($coreOk) {
        $demoFiles = @("$DemoSrc\RobustnessDemo.scala", "$DemoSrc\StreamingFallbackDemo.scala", "$DemoSrc\ConformalRun.scala")
        & $ScalacBin -classpath "$SparkCp;$BuildDir" -d $BuildDir @demoFiles
        $demoOk = $LASTEXITCODE -eq 0
    }
    if ($coreOk -and $demoOk) {
        Write-Host "SUCCESS: core library and demo drivers compiled."
        $StageStatus[2] = "SUCCESS"
    } else {
        Write-Host "FAILED: compilation error -- see output above."
        $StageStatus[2] = "FAILED"
        Write-Host "`n=== Aborting: stage 2 failed. ==="
        exit 1
    }
}
# NOTE: smartagri\kafka\IoTKafkaProducer.scala and IoTKafkaConsumer.scala require the real
# org.apache.kafka:kafka-clients jar (declared in build.sbt). If it isn't resolvable in your
# environment, they are intentionally excluded above; `sbt compile` with normal internet access
# compiles the whole project including those two files.

$JvmOpts = @(
    "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED", "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "-Xmx3g"
)

Push-Location $ProjectRoot
try {
    # ---- Stage 3: Kalman robustness ----
    Write-Host "`n--- [3/7] Adaptive vs. Static Kalman robustness demonstration ---"
    $kalmanOut = "$ResultsDir\kalman\robustness_summary.csv"
    if (Test-NonEmpty $kalmanOut) {
        Write-Host "SKIPPED: valid output already exists at $kalmanOut."
        $StageStatus[3] = "SKIPPED"
    } else {
        & java @JvmOpts -cp "$SparkCp;$BuildDir" RobustnessDemo
        if ($LASTEXITCODE -eq 0 -and (Test-NonEmpty $kalmanOut)) { Write-Host "SUCCESS."; $StageStatus[3] = "SUCCESS" }
        else { Write-Host "FAILED: RobustnessDemo did not produce valid output."; $StageStatus[3] = "FAILED" }
    }

    # ---- Stage 4: streaming fallback ----
    Write-Host "`n--- [4/7] IoT simulator -> streaming -> Kalman demonstration ---"
    $kafkaLive = $false
    try {
        $conn = Test-NetConnection -ComputerName "localhost" -Port 9092 -WarningAction SilentlyContinue -InformationLevel Quiet
        $kafkaLive = $conn
    } catch { $kafkaLive = $false }
    if ($kafkaLive) {
        Write-Host "Kafka broker reachable at localhost:9092 -- live mode available (producer/consumer still require the kafka-clients jar on the runtime classpath)."
    } else {
        Write-Host "Kafka live mode unavailable; local streaming-compatible mode used."
    }
    $streamOut = "$ResultsDir\streaming\fallback_demo_summary.csv"
    if (Test-NonEmpty $streamOut) {
        Write-Host "SKIPPED: valid output already exists at $streamOut."
        $StageStatus[4] = "SKIPPED"
    } else {
        & java @JvmOpts -cp "$SparkCp;$BuildDir" StreamingFallbackDemo
        if ($LASTEXITCODE -eq 0 -and (Test-NonEmpty $streamOut)) { Write-Host "SUCCESS."; $StageStatus[4] = "SUCCESS" }
        else { Write-Host "FAILED: StreamingFallbackDemo did not produce valid output."; $StageStatus[4] = "FAILED" }
    }

    # ---- Stage 5: ML + conformal + decision ----
    Write-Host "`n--- [5/7] Spark ML training/evaluation, conformal calibration, decision engine ---"
    $conformalOut = "$ResultsDir\conformal\coverage_report.csv"
    $decisionsOut = "$ResultsDir\decisions\decisions.csv"
    $mlMetricsOut = "$ResultsDir\ml\metrics.csv"
    if ((Test-NonEmpty $conformalOut) -and (Test-NonEmpty $decisionsOut) -and (Test-NonEmpty $mlMetricsOut)) {
        Write-Host "SKIPPED: valid persisted output already exists -- not recomputing expensive ML training."
        Write-Host "  $mlMetricsOut -- NOTE: verification-scale metrics (see run_metadata.csv); not regenerated here."
        $StageStatus[5] = "SKIPPED"
    } else {
        Write-Host "Valid output not fully present -- running ConformalRun (the expensive stage)."
        & java @JvmOpts -cp "$SparkCp;$BuildDir" ConformalRun
        if ($LASTEXITCODE -eq 0 -and (Test-NonEmpty $conformalOut) -and (Test-NonEmpty $decisionsOut)) { Write-Host "SUCCESS."; $StageStatus[5] = "SUCCESS" }
        else { Write-Host "FAILED or interrupted. Re-run this script -- stages 3/4 will SKIP and this stage will retry."; $StageStatus[5] = "FAILED" }
    }

    # ---- Stage 6: dashboard ----
    Write-Host "`n--- [6/7] Generating results dashboard ---"
    python "$ProjectRoot\dashboard.py"
    if ($LASTEXITCODE -eq 0 -and (Test-NonEmpty "$ResultsDir\dashboard\index.html")) {
        Write-Host "SUCCESS: dashboard written to $ResultsDir\dashboard\index.html"
        $StageStatus[6] = "SUCCESS"
    } else {
        Write-Host "FAILED: dashboard.py did not produce index.html."
        $StageStatus[6] = "FAILED"
    }
} finally {
    Pop-Location
}

# ---- Stage 7: final summary ----
Write-Host "`n--- [7/7] Final summary ---"
$overall = "SUCCESS"
foreach ($i in 1..6) {
    $s = $StageStatus[$i]
    if (-not $s) { $s = "NOT_RUN" }
    Write-Host "  Stage ${i}: $s"
    if ($s -eq "FAILED") { $overall = "FAILED" }
}
Write-Host "`n=== Overall: $overall. Results in $ResultsDir, dashboard at $ResultsDir\dashboard\index.html ==="
if ($overall -ne "SUCCESS") { exit 1 }
