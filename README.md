# 🌾 Smart Agriculture Big Data

**End-to-end crop-stress monitoring on a Dockerised Hadoop cluster: 47 GB of Sentinel-2 satellite imagery, ERA5 weather, SoilGrids soil data and live IoT streams, processed entirely in Scala on Spark, Hadoop MapReduce and Kafka.**

![Scala](https://img.shields.io/badge/Scala-2.12-DC322F?logo=scala&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-66CCFF?logo=apachehadoop&logoColor=black)
![Kafka](https://img.shields.io/badge/Kafka-3.8%20(KRaft)-231F20?logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-246%2F246%20passing-brightgreen)

---

## At a glance

| | |
|---|---|
| **Problem** | Detect crop stress and disease risk early for two Sentinel-2 tiles near Bengaluru (T43PGQ, T43PHQ) |
| **Data** | 41 Sentinel-2 L2A products (**47 GB raw**) + ERA5 weather + SoilGrids soil + simulated IoT sensor streams |
| **Cluster** | 1 NameNode + **5 DataNodes** (replication 3), YARN with 2 NodeManagers, Kafka, Spark History server |
| **Output** | 393,340-row multimodal table, T+1 stress-prediction models with conformal uncertainty, real-time crop alerts, HTML dashboard |
| **Language** | Scala for every processing step |
| **Quality** | 246 unit tests across 30 suites, plus an automated 23-check data-leakage audit |
| **HDFS health** | `fsck`: HEALTHY, 2,456 blocks, 0 under-replicated |

---

## Architecture

```
 data/ (downloads) ──hdfs dfs -put──▶ HDFS raw zone (5 DataNodes, rf=3)
                                        │
          Spark on YARN (Scala)         ▼
   Sentinel2Preprocess ─┐   Era5Preprocess ─┐   SoilGridsPreprocess ─┐
                        ▼                   ▼                        ▼
                HDFS processed zone: sentinel2_ndvi / era5_weather / soilgrids (Parquet)
                        │
                        ├─▶ CanonicalHistoricalDatasetBuilder ─▶ canonical_historical (393,340 rows)
                        │        ├─▶ Hadoop MapReduce (NdviTileMonthDriver) ─▶ tile × month NDVI
                        │        ├─▶ FeaturePipelineBuilder (leakage-safe, T+1 target) ─▶ ml_features
                        │        │        └─▶ TrainEvaluateJob: RF + baselines + conformal + decisions
                        │        └─▶ RobustnessExperiment (adaptive vs static Kalman)
                        │
 IoTFieldSimulator ─▶ Kafka (iot_sensor_readings) ─▶ CropHealthStreamingJob (Structured Streaming)
   5 sensor types,        per-sensor adaptive Kalman state + satellite stress probability
   faults injected        + soil context from HDFS ─▶ Kafka (crop_alerts) + HDFS
                                        │
                  LeakageAudit  ·  DashboardReport (results/dashboard/index.html)
```

**Stack:** Hadoop 3.2.1 (HDFS, YARN, MapReduce) · Spark 3.5 (SQL, MLlib, Structured Streaming) · Kafka 3.8 · Scala 2.12 / sbt · Docker Compose

---

## What it does

### 1. Big-data ingestion and preprocessing (HDFS → HDFS)

| Source | Highlights |
|---|---|
| **Sentinel-2** | Selective in-place extraction from zips on HDFS (reads **2.86 GB of 49.3 GB**), pure-Java JPEG2000 decoding, radiometric correction, per-pixel cloud/shadow/water masking, NDVI over **1.21 billion pixels**, aggregation to a 100×100 grid (~1.1 km cells), UTM → WGS84 georeferencing, quality filtering |
| **ERA5** | In-memory unzip, NetCDF4/HDF5 decoding, CF time handling, validation, daily aggregation, derived wind speed / relative humidity / VPD |
| **SoilGrids** | GeoTIFF decoding, resampling to the cell grid with gap-filling, texture normalisation, USDA texture class |

25 documented operations in total. Outputs: **393,340** NDVI cell observations, **29,565** daily weather rows, **19,916** soil profiles, joined into one canonical multimodal table.

### 2. Machine learning with honest evaluation

- **Target:** stress at the *next* satellite observation (T+1), predicted only from information available at T
- **Split:** chronological (cut-off 2024-04-07), 105,848 train / 48,747 test rows, no random splitting
- **Models:** random forest, gradient boosting, logistic regression, with naive-persistence and majority-class baselines on the identical split
- **Uncertainty:** split conformal prediction (α = 0.10) and a decision engine (*InvestigatePriority* fields were stressed 55% of the time vs 22% for *LowRisk*)

### 3. Real-time IoT stream

`IoTFieldSimulator` → Kafka → Spark Structured Streaming with a per-sensor adaptive Kalman filter (`flatMapGroupsWithState`) → alerts on `crop_alerts`. Against the simulator's hidden conditions it detected **32/33** drying fields and **26/26** fungal-risk fields with **0/141** false alarms.

### 4. Engineering rigour

- **Automated leakage audit (23/23 checks):** reflection on compiled types, scope-aware source scans, temporal checks, train-only checks, and **5 positive controls** (planted violations must be caught)
- **Resumable pipeline:** stages with existing HDFS output are skipped; `FORCE=1` and `ONLY="9 11"` give control
- **Evidence trail:** every stage logs to `docs/evidence/`, with HDFS/YARN reports and 16 UI screenshots

---

## Results (reported as measured)

### Adaptive Kalman filter (Monte-Carlo: 2,000 series × 11 scenarios × 4 variants)

`R_t = R_base / max(reliability_t, ε)` where reliability uses only observable signals (physical bounds, deviation from recent history), never ground truth.

| Scenario | Static RMSE | Adaptive RMSE | Change |
|---|---|---|---|
| Outliers 5% / 10% / 15% / 20% | 0.042 / 0.061 / 0.075 / 0.087 | 0.018 / 0.025 / 0.035 / 0.046 | **−58% / −58% / −54% / −47%** |
| Bias 10% / 20% | 0.025 / 0.039 | 0.019 / 0.030 | **−26% / −22%** |
| Mixed faults | 0.066 | 0.036 | **−46%** |
| Clean data | 0.0108 | 0.0124 | +15% (worse) |
| Missing 10% / 20% | 0.0122 / 0.0137 | 0.0137 / 0.0151 | +13% / +10% (worse) |

An ablation shows the bounds check never hurts, while the deviation term drives both the gains and the clean-data loss. On real Sentinel-2 series: −35% RMSE at 15% outliers, but +101% on clean data. **Recommendation:** enable the deviation term only for streams known to be faulty.

### Next-observation stress prediction (test period)

| Model | Accuracy | ROC-AUC | Balanced acc. |
|---|---|---|---|
| Naive persistence | 0.701 | 0.765 | 0.661 |
| Random forest + current-anomaly feature | 0.696 | 0.727 | 0.653 |
| Gradient boosting (chosen on validation) | 0.675 | 0.675 | 0.625 |
| Logistic regression | 0.639 | 0.707 | 0.500 |
| Majority class | 0.361 | 0.500 | 0.500 |

**No learned model beats the naive baseline on this data**, and the baseline was added specifically to catch this. Validation ROC-AUC was 0.894, so the drop to 0.675 measures temporal drift (stressed rate 70% in the fit period vs 36% in test). Conformal coverage is 80.7% against a 90% target for the same reason.

---

## Quick start

**Prerequisites:** Docker Desktop (16 GB WSL memory is enough) and Git Bash / WSL / Linux / macOS.

```bash
# 0. Put downloads in data/sentinel2/*.zip, data/era5/*.zip, data/soilgrids/*.tif

# 1. Build the Scala fat jar (sbt runs inside Docker)
docker run --rm -v "$PWD/scala-ingestion:/app" -v sbt_cache:/root/.cache \
  -v sbt_ivy:/root/.ivy2 -v sbt_home:/root/.sbt -w /app \
  sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.12.21 sbt -batch assembly

# 2. Start the cluster: NameNode, 5 DataNodes, YARN, Spark client, Kafka
docker compose up -d

# 3. Run the full pipeline (resumable)
bash scripts/run_pipeline.sh

# 4. Optional: capture UI screenshots (PowerShell)
powershell -ExecutionPolicy Bypass -File scripts/capture_screenshots.ps1
```

Full run: about 20 minutes, plus a one-time ~37-minute upload of the 47 GB raw data. Run tests with `sbt test`.

**Web UIs:** NameNode `localhost:9870` · YARN `localhost:8088` · Spark History `localhost:18080` · Dashboard `results/dashboard/index.html`

### Pipeline stages

| Stage | Job | Runs on |
|---|---|---|
| 00 | Cluster health check | HDFS |
| 01 | Raw upload | HDFS |
| 02–04 | Sentinel-2 / ERA5 / SoilGrids preprocessing | Spark on YARN |
| 05 | Canonical multimodal dataset | Spark on YARN |
| 06 | NDVI tile × month aggregation | Hadoop MapReduce on YARN |
| 07 | Leakage-safe features + T+1 target | Spark on YARN |
| 08 | Kalman robustness + ablation | Spark on YARN |
| 09 | Models + baselines + conformal + decisions | Spark MLlib on YARN |
| 10 | Leakage audit | JVM |
| 11 | IoT simulator → Kafka → streaming → alerts | Kafka + Spark Streaming |
| 12 | HDFS/YARN evidence capture | HDFS / YARN |
| 13 | Dashboard | JVM |

---

## Cluster evidence

| NameNode: 5 live DataNodes | HDFS raw Sentinel-2 data |
|---|---|
| ![NameNode](docs/evidence/screenshots/02_namenode_5_datanodes.png) | ![HDFS raw](docs/evidence/screenshots/04_hdfs_raw_sentinel2_48GB.png) |

More in `docs/evidence/`: `dfsadmin_report.txt`, `fsck_raw_blocks.txt`, `fsck_processed_blocks.txt`, `hdfs_du.txt`, YARN application list, Kafka topic list.

---

## Repository layout

```
docker-compose.yml, hadoop.env      Cluster definition (5 DataNodes, rf=3, YARN, Spark client, Kafka)
cluster/                            Hadoop client and spark-defaults config
scala-ingestion/                    All Scala code (sbt); tests in src/test
  smartagri/preprocess/             Sentinel-2, ERA5, SoilGrids preprocessing
  smartagri/canonical/              Multimodal join + validation
  smartagri/mapreduce/              Classic Hadoop MapReduce job
  smartagri/features/               Leakage-safe features, T+1 target, chronological split
  smartagri/kalman/                 Adaptive Kalman, Spark propagation, robustness experiment
  smartagri/ml, conformal, decision Models, conformal prediction, decision engine
  smartagri/iot, kafka, streaming   IoT simulator, Kafka producer, Structured Streaming job
  smartagri/audit, report           Leakage audit, dashboard generator
scripts/                            run_pipeline.sh, hdfs_load_raw.sh, submit.sh, capture_*.sh/.ps1
results/                            CSV reports + dashboard (mirrored in HDFS /user/smart_agri/results)
docs/evidence/                      Stage logs, HDFS/YARN evidence, screenshots
legacy/                             Superseded Python / old Scala code (not built)
```

---

## Limitations and roadmap

1. **ERA5 covers 2023 only**, so the weather features are constants in the 2024 ML rows. Current model results are effectively satellite + soil. Fix: add more ERA5 files to `data/era5/` and re-run with `FORCE=1 ONLY="1 3 5 7 9 11 13"`.
2. **SoilGrids and ERA5 stop at longitude 78.0°**, while tile T43PHQ extends to 78.76°, leaving 7,480 cells without soil data. Fix: download both for longitude 76–79.
3. **IoT data is simulated.** Real devices only need to publish the same JSON to `iot_sensor_readings`.
4. **Single-host cluster.** Replication and YARN distribution are real; physical fault tolerance is not.
5. **Temporal drift.** Training covers only 4 fit dates; a sensitivity run with lower history requirements is exploratory (`results/experiments/`).

---

## Author

**Navaddip Chowdary**: B.Tech CSE (AI), Amrita Vishwa Vidyapeetham
GitHub: [@navaddip](https://github.com/navaddip)
