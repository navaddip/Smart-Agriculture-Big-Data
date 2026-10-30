# processed/ (dataset placeholder)

The dataset Parquet files are intentionally NOT included in this zip
(you already have them locally, per your request). Place these three
files here before running the project:

- sentinel2_ndvi.parquet
- era5_weather.parquet
- soilgrids.parquet

These are the same three files referenced throughout the codebase
(Task 3B's CanonicalHistoricalDatasetBuilder, Task 10's
MlDataPreparation, scripts/demo_src/ConformalRun.scala, etc.) via the
relative path "processed/<name>.parquet" -- so as long as this folder
sits at the project root alongside scala-ingestion/, scripts/, and
dashboard.py, everything will find them correctly.
