# Smart Agriculture — Scala Preprocessing (Spark + Hadoop)

Scala/Spark ingestion pipeline that reads raw geospatial data from HDFS,
performs preprocessing, and writes processed Parquet tables back to HDFS.

## Modules (src/main/scala/smartagri)

| File | Modality | Library | Preprocessing operations |
|------|----------|---------|--------------------------|
| `SoilGridsIngestion.scala` | SoilGrids soil properties | GeoTrellis (GeoTIFF) | decode raster, reproject sample points, nearest-pixel sampling, unit conversion, null-clean coverage gaps, write Parquet |
| `Era5Ingestion.scala` | ERA5 weather | NetCDF-Java (GRIB) | stage GRIB from HDFS, decode variables over the time axis, flatten to (date, lat, lon, variable, value), write Parquet |
| `SentinelIngestion.scala` | Sentinel-2 imagery | jai-imageio (JPEG-2000) | stream .SAFE zip, decode B04/B08 bands, apply radiometric offset, compute NDVI, block-average to a grid, write Parquet |
| `HdfsUtils.scala` | — | Hadoop FS | shared helpers to read bytes / stage files from HDFS |

Input:  `hdfs://namenode:9000/user/smart_agri/raw/{soilgrids,era5,sentinel2}`
Output: `hdfs://namenode:9000/user/smart_agri/processed/*_scala.parquet`

## Build (fat jar) — runs sbt in a container, no local install

    docker run -it --rm -v "D:/SEM-5/BIG_DATA/scala-ingestion:/app" -w /app \
      sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.9.9_2.12.18 sbt assembly

Produces: target/scala-2.12/smart-agri-ingestion-assembly-1.0.jar

## Run one module against HDFS (spark-submit in the Spark container)

    docker run -it --rm --network big_data_default \
      -v "D:/SEM-5/BIG_DATA/scala-ingestion/target/scala-2.12:/jars" \
      apache/spark:3.5.3 /opt/spark/bin/spark-submit \
      --class smartagri.SoilGridsIngestion --master "local[*]" \
      /jars/smart-agri-ingestion-assembly-1.0.jar

Swap `--class` for `smartagri.Era5Ingestion` or `smartagri.SentinelIngestion`.

## Status
- SoilGrids: primary/demo path (GeoTrellis toolchain verified resolving).
- ERA5 / Sentinel: complete but GRIB/JP2 JVM decoding may need tuning on first run.
