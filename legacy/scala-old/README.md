# Smart Agriculture Scala Preprocessing

## SoilGrids stage

`soilgrids.scala`:

1. Reads the Sentinel-2 NDVI grid from HDFS.
2. Reads all seven SoilGrids GeoTIFFs directly from HDFS.
3. Decodes each GeoTIFF with GeoTrellis.
4. Samples the SoilGrids rasters onto the NDVI grid using nearest-neighbour sampling.
5. Converts SoilGrids integer values using the project conversion factors.
6. Converts raw value `0` to null because it represents uncovered soil-tile areas.
7. Writes `soilgrids.parquet` to HDFS.
8. Reads the output back for verification.

Expected HDFS input:

`hdfs://namenode:9000/user/smart_agri/raw/soilgrids/`

Expected NDVI input:

`hdfs://namenode:9000/user/smart_agri/processed/sentinel2_ndvi.parquet`

Output:

`hdfs://namenode:9000/user/smart_agri/processed/soilgrids.parquet`

## Run

Start the Hadoop/Docker cluster first.

From Windows CMD:

```bat
run_soilgrids.bat
```

From PowerShell:

```powershell
.\run_soilgrids.ps1
```

## Important CRS check

The program prints the CRS of every SoilGrids raster.

The sampling function assumes the raster coordinates are longitude/latitude and therefore compatible with the NDVI `lon`/`lat` columns.

Before accepting the final output, check the printed CRS. If the rasters use a projected CRS rather than geographic WGS84 coordinates, the sampling function must be changed to reproject the NDVI coordinates first.
