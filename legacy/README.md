# Legacy code (superseded, not built, not run)

Kept only for traceability of the project's history. Everything here has been replaced by
Scala/Spark jobs that read raw data from HDFS and write processed data back to HDFS:

| Legacy | Replaced by |
|---|---|
| `python/Extract_ndvi.py` | `smartagri.preprocess.Sentinel2Preprocess` |
| `python/extract_era5.py` | `smartagri.preprocess.Era5Preprocess` |
| `python/extract_soilgrids.py` | `smartagri.preprocess.SoilGridsPreprocess` |
| `python/kalman_fusion.py` | `smartagri.kalman.KalmanFusionPipeline` / `RobustnessExperiment` |
| `python/conformal_irrigation.py` | `smartagri.ml.TrainEvaluateJob` (conformal + decisions) |
| `python/iot_producer.py`, `iot_consumer.py` | `smartagri.iot.IoTFieldSimulator`, `smartagri.streaming.CropHealthStreamingJob` |
| `scala-ingestion/SentinelIngestion.scala` (read JP2 as GeoTIFF, driver-only, wrote 0 rows) | `smartagri.preprocess.Sentinel2Preprocess` |
| `scala-ingestion/Era5Ingestion.scala`, `SoilGridsIngestion.scala`, `Analytics.scala` | `smartagri.preprocess.*`, `smartagri.ml.TrainEvaluateJob` |
| `demo_src/*.scala`, `run_demo.sh/.ps1` (1-core local sandbox demos) | `scripts/run_pipeline.sh` on the YARN cluster |
| `scala-old/` | `scala-ingestion/` |
