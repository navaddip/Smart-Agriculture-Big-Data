docker run --rm --user root --network big_data_default `
  -v "D:/SEM-5/BIG_DATA/scala:/scripts" `
  apache/spark:3.5.3 `
  /opt/spark/bin/spark-submit `
  --master "local[*]" `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages "org.locationtech.geotrellis:geotrellis-raster_2.12:3.7.1" `
  --class SoilGridsPreprocess `
  /scripts/soilgrids.scala
