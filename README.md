# Smart Agriculture Big Data Project

Crop-stress and crop-disease monitoring for two Sentinel-2 tiles near Bengaluru (T43PGQ, T43PHQ).
The project fuses satellite (Sentinel-2 L2A), weather (ERA5), soil (SoilGrids) and live IoT sensor
streams. **Every processing step is Scala on Spark / Hadoop MapReduce / Kafka**, running on a
Dockerised Hadoop cluster. Raw data and all processed data live in HDFS.

---

## 1. Mandatory requirements: where each one is met

| # | Requirement | Where / evidence |
|---|---|---|
| 1 | Big dataset (40 GB+) | 41 Sentinel-2 L2A zips = **47.0 GB raw in HDFS** (141 GB on disk at replication 3) + ERA5 + SoilGrids. `docs/evidence/hdfs_yarn/hdfs_du.txt`, screenshot `04_hdfs_raw_sentinel2_48GB.png` |
| 2 | Hadoop cluster with ≥ 2 DataNodes | **1 NameNode + 5 DataNodes, replication factor 3**, YARN ResourceManager + 2 NodeManagers + history server (`docker-compose.yml`, `hadoop.env`). `dfsadmin_report.txt`, screenshot `02_namenode_5_datanodes.png` |
| 3 | Raw dataset in HDFS | `/user/smart_agri/raw/{sentinel2,era5,soilgrids}` loaded by `scripts/hdfs_load_raw.sh`. `fsck_raw_blocks.txt`: every block on 3 DataNodes |
| 4 | Scala preprocessing code | `scala-ingestion/src/main/scala/smartagri/preprocess/` (`Sentinel2Preprocess`, `Era5Preprocess`, `SoilGridsPreprocess`), run as Spark-on-YARN jobs |
| 5 | Preprocessing operations performed | See §3: 25 operations, each logged in `docs/evidence/02-04_*.log` and reported in `results/preprocessing/*.csv` |
| 6 | Processed dataset | 393,340 cloud-masked NDVI cell observations, 29,565 daily weather rows, 19,916 soil profiles → 393,340-row multimodal canonical table |
| 7 | Processed dataset stored back in HDFS | `/user/smart_agri/processed/*.parquet` (replication 3). `fsck_processed_blocks.txt`, screenshots `07`–`09` |
| 8 | Evidence / screenshots of HDFS storage and execution | `docs/evidence/screenshots/` (16 PNGs of the NameNode, HDFS browser, YARN, Spark History and dashboard), `docs/evidence/hdfs_yarn/` (dfsadmin, fsck, du, yarn application list, Kafka topics), `docs/evidence/NN_*.log` (full log of every stage) |

`fsck` of `/user/smart_agri`: **Status HEALTHY, 5 DataNodes, 2,456 blocks, 100% minimally replicated, 0 under-replicated, average block replication 3.0.**

---

## 2. Architecture

```
 data/ (downloads) ──hdfs dfs -put──▶ HDFS raw zone (5 DataNodes, rf=3)
                                        │
          Spark on YARN (Scala)         ▼
   Sentinel2Preprocess ─┐   Era5Preprocess ─┐   SoilGridsPreprocess ─┐
                        ▼                   ▼                        ▼
                HDFS processed zone: sentinel2_ndvi / era5_weather / soilgrids (Parquet)
                        │
                        ├─▶ CanonicalHistoricalDatasetBuilder ─▶ canonical_historical (393,340 rows)
                        │        ├─▶ NdviFlatInputPreparation ─▶ Hadoop MapReduce (NdviTileMonthDriver) ─▶ tile×month NDVI
                        │        ├─▶ FeaturePipelineBuilder (leakage-safe, T+1 target) ─▶ ml_features
                        │        │        └─▶ TrainEvaluateJob: RF + baselines + conformal + decisions ─▶ HDFS results/
                        │        └─▶ RobustnessExperiment (adaptive vs static Kalman, Monte-Carlo + real NDVI)
                        │
 IoTFieldSimulator ─▶ Kafka topic iot_sensor_readings ─▶ CropHealthStreamingJob (Structured Streaming on YARN)
   (5 sensor types,        per-sensor adaptive Kalman state (flatMapGroupsWithState)
    faults injected)       + satellite stress probability + soil context from HDFS
                           ─▶ Kafka topic crop_alerts  +  HDFS streaming/alerts, streaming/iot_filtered
                                        │
                  LeakageAudit (reflection + scope-aware scan + positive controls)
                  DashboardReport (Scala → results/dashboard/index.html)
```

Stack: Hadoop 3.2.1 (HDFS + YARN + MapReduce), Spark 3.5 (Scala 2.12, Spark SQL / MLlib /
Structured Streaming), Kafka 3.8 (KRaft). Everything is Scala. The only non-Scala file is
`scripts/capture_screenshots.ps1`, which drives the browser for the screenshots.

---

## 3. Preprocessing operations (all Scala / Spark, HDFS → HDFS)

**Sentinel-2** (`Sentinel2Preprocess`): one Spark task per product on the YARN executors.
1. Product inventory and file-name validation.
2. **De-duplication**: the byte-identical `… (1).zip` copy is detected by HDFS checksum and dropped, and superseded re-processings of the same tile/time are handled.
3. **Selective extraction**: each zip is opened in place over HDFS positional reads, and only B04, B8A, SCL and 2 XML files are read. That is **2.86 GB read out of 49.3 GB**.
4. **JPEG2000 decoding** in pure Java (the JP2 box parser works around a JJ2000 64-bit box-length bug).
5. **Radiometric correction** (DN → reflectance with each product's own `BOA_ADD_OFFSET` / quantification).
6. **Cloud, cloud-shadow and water masking** from the Scene Classification Layer, per pixel. The computed cloud % matches ESA's own metadata.
7. NDVI per pixel, **1.21 billion pixels** in total.
8. **Spatial aggregation** to a 100×100 grid per tile (~1.1 km cells): mean, std, clear/cloud/water fractions.
9. **Georeferencing**: UTM → WGS84 in Scala, matching pyproj to 1e-7°.
10. **Quality filtering**: a cell needs ≥ 30% clear land pixels.

**ERA5** (`Era5Preprocess`):
11. In-memory unzip of the CDS download.
12. NetCDF4/HDF5 decoding (NetCDF-Java) with CF scale/offset/fill.
13. CF time decoding.
14. Instant + accumulated streams merged.
15. Physical-range validation.
16. Negative accumulations clipped.
17. Duplicate timestamps removed.
18. **Daily aggregation** (mean / sum / min / max).
19. Unit conversion.
20. **Derived variables**: wind speed, relative humidity, VPD.
21. Completeness report.

**SoilGrids** (`SoilGridsPreprocess`):
22. GeoTIFF decoding and nodata handling.
23. **Resampling**: the mean of all soil pixels inside each 1.1 km cell footprint, plus **gap-filling** from 2× and 4× windows.
24. Unit conversion, and clay + sand + silt re-normalised to 100%.
25. Derived **USDA texture class**.

The **canonical join** (`CanonicalHistoricalDatasetBuilder`) matches ERA5 by nearest neighbour and exact date, and joins soil 1:1 by cell, with validation checks.

---

## 4. How to run (start to end)

Prerequisites: Docker Desktop (16 GB WSL memory is enough), Git Bash (or WSL/Linux/macOS). Nothing else needs installing.

```bash
# 0. put the downloads in data/sentinel2/*.zip, data/era5/*.zip, data/soilgrids/*.tif
# 1. build the Scala fat jar (sbt runs inside Docker)
docker run --rm -v "$PWD/scala-ingestion:/app" -v sbt_cache:/root/.cache -v sbt_ivy:/root/.ivy2 -v sbt_home:/root/.sbt -w /app sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.12.21 sbt -batch assembly
# 2. start the cluster: NameNode, 5 DataNodes, YARN, Spark client, Kafka
docker compose up -d
# 3. run the whole pipeline: raw -> HDFS -> preprocessing -> ... -> streaming -> evidence -> dashboard
bash scripts/run_pipeline.sh
# 4. screenshots of the web UIs (PowerShell)
powershell -ExecutionPolicy Bypass -File scripts/capture_screenshots.ps1
```

The pipeline is **resumable**: stages whose HDFS output exists are skipped. `FORCE=1` re-runs everything, and `ONLY="9 11"` runs selected stages. Each stage logs to `docs/evidence/NN_<stage>.log`. On this machine the full run took about 20 minutes, plus ~37 minutes for the one-time 47 GB raw upload.

(Stage 00 also stages Spark's jars in HDFS for YARN, once.)

Web UIs: NameNode http://localhost:9870 · YARN http://localhost:8088 · Spark History http://localhost:18080 · a running job http://localhost:4040.
Final report: `results/dashboard/index.html`.

| Stage | Job | Runs on |
|---|---|---|
| 00 | cluster health (5 DataNodes, rf=3) | HDFS |
| 01 | raw upload | HDFS |
| 02–04 | Sentinel-2 / ERA5 / SoilGrids preprocessing | Spark on YARN |
| 05 | canonical multimodal dataset | Spark on YARN |
| 06 | NDVI tile × month aggregation | **Hadoop MapReduce on YARN** |
| 07 | leakage-safe features + T+1 target | Spark on YARN |
| 08 | Kalman robustness + ablation | Spark on YARN |
| 09 | RF + baselines + conformal + decisions | Spark MLlib on YARN |
| 10 | leakage audit | JVM |
| 11 | IoT simulator → Kafka → Structured Streaming → `crop_alerts` | Kafka + Spark Streaming on YARN |
| 12 | HDFS/YARN evidence capture | HDFS / YARN |
| 13 | dashboard | JVM |

Unit tests: `sbt test` passes **246 / 246** across 30 suites (`docs/evidence/02_unit_tests.log`).

---

## 5. Novelties (measured on this run, not asserted)

Kalman filtering, Random Forests, Kafka/Structured Streaming and split conformal prediction are
all established techniques and **none is claimed as a novel algorithm**. What is new is how they
are adapted, combined and validated for this multimodal agricultural problem. The numbers below
come from `results/` and are shown with their weaknesses.

### 1. Adaptive, reliability-driven Kalman measurement noise
`R_t = R_base / max(reliability_t, ε)`. Reliability comes only from observable signals: a physical-bounds check, and deviation from the recent local history. It never uses ground truth. The experiment is a Monte-Carlo run on YARN: **2,000 independent series × 11 scenarios × 4 variants**, with 95% CIs on the paired difference (`results/kalman/robustness_summary.csv`).

| Scenario | Static RMSE | Adaptive RMSE | Change | Verdict |
|---|---|---|---|---|
| outliers 5% / 10% / 15% / 20% | 0.042 / 0.061 / 0.075 / 0.087 | 0.018 / 0.025 / 0.035 / 0.046 | **−58% / −58% / −54% / −47%** | significantly better |
| bias 10% / 20% | 0.025 / 0.039 | 0.019 / 0.030 | **−26% / −22%** | significantly better |
| mixed (10% each) | 0.066 | 0.036 | **−46%** | significantly better |
| clean | 0.0108 | 0.0124 | +15% | significantly **worse** |
| missing 10% / 20% | 0.0122 / 0.0137 | 0.0137 / 0.0151 | +13% / +10% | significantly **worse** |

The **ablation** (`ablation_study.csv`) explains this trade-off:
- The *bounds* component never hurts: on clean and missing data it equals the static filter.
- The *deviation* component produces all of the bias gain and all of the clean/missing loss.
- Only the two together reach the full outlier gain.

On **real Sentinel-2 NDVI series** from HDFS (5,000 cells, injected corruption; `real_ndvi_injection.csv`):
- 15% outliers: **−35% RMSE** (better).
- Clean data: **+101%** (worse). Real crop NDVI changes quickly between observations, and the deviation term mistakes genuine change for unreliability.

Practical rule: switch the deviation term on only when the stream is known to be faulty.
*(The earlier guide claimed "76% / 29%" from a single 60-point series. Those numbers are superseded by this larger experiment.)*

### 2. Partition-local sequential Kalman propagation on Spark
Kalman recursion depends on the previous step's *output*, so it cannot be a Spark `Window` function. Instead, the data is repartitioned by cell, sorted within each partition, and passed through `mapPartitions`, with a **streaming iterator that buffers one cell at a time**. Memory is bounded by the longest single-cell history, never by partition size. There is no `collect()`, and no `.toList` of the partition (the earlier version had that; it is fixed now). In the live stream, the same filter keeps its state per sensor across micro-batches with `flatMapGroupsWithState`.

### 3. Non-circular future-stress target
The target is stress at the **next** Sentinel observation (T+1), predicted from information up to T. The baseline is frozen strictly before T, the threshold is chosen on training data only, and the split is chronological at 2024-04-07. Sizes: 174,462 eligible rows → 105,848 train / 48,747 test (19,867 rows that straddle the cutoff are excluded).

### 4. Automated, self-verifying leakage audit
`smartagri.audit.LeakageAudit` passes **23 / 23 checks**:
- 5 structural checks via reflection on compiled types, e.g. the Kafka/stream type `ObservableReading` provably has no health or ground-truth field.
- 4 feature-set checks.
- 3 scope-aware source scans: `future_*` must exist in target construction and is forbidden in ml/, streaming/, decision/, conformal/, kalman/.
- 3 temporal checks: every `rowsBetween` ends at −1, `lead()` appears only in target construction, and there is no random split.
- 3 train-only checks: target statistics, conformal calibration, and model selection on a validation date inside the fit period.
- **5 positive controls**: planted violations must be caught, or the audit fails.

Report: `results/leakage_audit/leakage_audit_report.csv`.

### 5. System-level multimodal integration with honest scale accounting
Satellite + weather + soil + IoT, adaptive Kalman fusion, ML, conformal uncertainty and decision support run end-to-end on one cluster. Scale is stated precisely:
- **Volume**: 47 GB of raw input, of which only 2.86 GB (6%) actually has to be read.
- **Processed size**: tens of MB of Parquet.
- **Infrastructure**: 5 DataNodes and 2 NodeManagers on one 32-core host.

Baselines use the **identical split and features** (`results/ml/baseline_comparison.csv`, test period):

| Model | Accuracy | ROC-AUC | PR-AUC | Balanced acc. | Recall (stressed) |
|---|---|---|---|---|---|
| naive persistence ("stressed now → stressed next") | **0.701** | **0.765** | **0.657** | **0.661** | 0.517 |
| random forest + current-anomaly feature | 0.696 | 0.727 | 0.533 | 0.653 | 0.497 |
| logistic regression | 0.639 | 0.707 | 0.648 | 0.500 | 0.001 |
| **gradient boosting (primary, chosen on validation)** | 0.675 | 0.675 | 0.470 | 0.625 | 0.445 |
| random forest, original features | 0.629 | 0.656 | 0.459 | 0.530 | 0.174 |
| majority class | 0.361 | 0.500 | 0.361 | 0.500 | 1.000 |

**No learned model beats the naive baseline on this data.** This comparison was put in place precisely to catch this, so it is reported, not tuned away on the test set.

What the second round of modelling changed:
- Adding the current anomaly `z_T` as a feature and dropping the 9 constant weather columns lifted the tree models clearly: accuracy 0.63 → 0.68–0.70, and recall on stressed fields 0.17 → 0.45–0.50.
- Hyper-parameters and the primary model were chosen on a validation date inside the fit period (2024-03-18), never on the test period (`results/ml/model_selection.csv`). Validation picked gradient boosting (validation ROC-AUC 0.894).
- On the test period that model scores only 0.675, and the enhanced Random Forest does better (0.727). The drop from 0.89 to 0.68 measures **temporal drift**: what predicts stress in March does not carry over to April–June.

**Sensitivity experiment (exploratory, `results/experiments/`):** lowering the minimum-history requirement from 10 to 5 or 3 prior observations gives 9 or 11 fit dates, up from 4.
- With 5, logistic regression reaches accuracy 0.743 / ROC-AUC 0.782, the only run in which a learned model edges past the naive rule (0.701 / 0.765). Gradient boosting improves to 0.691 / 0.719.
- With 3, nothing improves.
- These runs were compared on the test period and logistic regression is unstable across settings, so this is a direction for future work, not a tuned result. The main results keep the approved value of 10.

The likely causes are all measured:
- Training covers only 4 fit dates.
- The stressed rate shifts from 70% (fit) to 36% (test).
- The weather features are empty (see §6).

The streaming side works well on its own task. Against the simulator's hidden field conditions it detected:
- **32 / 33** drying fields;
- **26 / 26** fungal-risk (humid) fields;
- with **0 / 141** false alarms (`results/streaming/detection_evaluation.csv`).

This is a pipeline-correctness check on simulated data with clearly separated conditions. It is not field validation.

Split conformal prediction (α = 0.10) on the primary model: **80.7% empirical coverage** against the 90% target, so it under-covers. Split conformal assumes the calibration and test periods are exchangeable, and the drift above breaks that. The decision engine's ordering is sensible: fields flagged *InvestigatePriority* turned out stressed 55% of the time, against 22% for *LowRisk*.

---

## 6. Known limitations (and how to fix them)

1. **ERA5 covers 2023 only.** Every ML-eligible row (≥ 10 prior observations) falls in 2024, so all 9 weather features are imputed constants in both train and test (`feature_pipeline_summary.csv`: `train_weather_available_rows = 0`). **Scope decision:** the project deliberately keeps the 2023 ERA5 download. ERA5 is still fully preprocessed, stored in HDFS, joined into the canonical dataset (the 98,382 rows from 2023 carry real weather) and reported. However, the future-stress model's result is effectively satellite + soil only, and it should be read that way. Extending it later only needs another ERA5 file in `data/era5/` and `FORCE=1 ONLY="1 3 5 7 9 11 13" bash scripts/run_pipeline.sh`, because the ERA5 job picks up every file in `raw/era5` automatically.
2. **SoilGrids and ERA5 stop at longitude 78.0°**, but tile T43PHQ runs to 78.76°. As a result, 7,480 T43PHQ cells have no soil data (reported as `missing_outside_extent`), and the ERA5 nearest-neighbour distance reaches 85 km there. **Fix:** download both for longitude 76–79.
3. There are no real IoT devices yet. `IoTFieldSimulator` stands in and publishes only observable values. Replacing it with real devices only requires publishing the same JSON to `iot_sensor_readings`.
4. It is a single-host cluster: the 5 DataNodes are containers on one machine. Replication and HDFS/YARN distribution are real, but the physical fault tolerance is not.

---

## 7. Repository layout

```
docker-compose.yml, hadoop.env      cluster definition (5 DataNodes, rf=3, YARN, Spark client, Kafka)
cluster/                            Hadoop client + spark-defaults config for the Spark client
scala-ingestion/                    all Scala code (sbt), tests under src/test
  smartagri/preprocess/             Sentinel-2, ERA5, SoilGrids preprocessing
  smartagri/canonical/              multimodal join + validation
  smartagri/mapreduce/              classic Hadoop MapReduce job
  smartagri/features/               leakage-safe features, T+1 target, chronological split
  smartagri/kalman/                 adaptive Kalman, Spark propagation, robustness experiment
  smartagri/ml, conformal, decision RF + baselines, conformal prediction, decision engine
  smartagri/iot, kafka, streaming   IoT simulator, Kafka producer, Structured Streaming job
  smartagri/audit, report           leakage audit, dashboard generator
scripts/                            run_pipeline.sh, hdfs_load_raw.sh, submit.sh, capture_*.sh/.ps1
results/                            CSV reports + dashboard (also mirrored in HDFS /user/smart_agri/results)
docs/evidence/                      stage logs, HDFS/YARN evidence, screenshots
legacy/                             superseded Python / old Scala code (not built)
```
