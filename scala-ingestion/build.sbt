name := "smart-agri-ingestion"
version := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// The YARN NodeManagers run Java 8 (bde2020 images); the build image runs JDK 17.
// -release 8 guarantees both the bytecode AND the referenced JDK API are Java-8 safe.
scalacOptions ++= Seq("-release", "8", "-deprecation", "-feature")

resolvers += "Unidata" at "https://artifacts.unidata.ucar.edu/repository/unidata-all/"

libraryDependencies ++= Seq(
  "org.apache.spark"            %% "spark-sql"             % sparkVersion % "provided",
  "org.apache.spark"            %% "spark-mllib"           % sparkVersion % "provided",
  "org.apache.hadoop"           %  "hadoop-client"         % "3.2.1"      % "provided",
  ("org.apache.spark"           %% "spark-sql-kafka-0-10"  % sparkVersion)
    .excludeAll(ExclusionRule("org.apache.hadoop")),   // Hadoop client comes from the cluster
  // GeoTIFF decoding (SoilGrids)
  "org.locationtech.geotrellis" %% "geotrellis-raster"     % "3.7.1",
  // JPEG2000 decoding (Sentinel-2 L2A band rasters) -- pure Java, runs inside YARN executors
  "com.github.jai-imageio"      %  "jai-imageio-jpeg2000"  % "1.4.0",
  "com.github.jai-imageio"      %  "jai-imageio-core"      % "1.4.0",
  // NetCDF4 / HDF5 decoding (ERA5) -- pure Java
  "edu.ucar"                    %  "cdm"                   % "4.5.5",
  "commons-io"                  %  "commons-io"            % "2.15.1",
  "org.apache.commons"          %  "commons-compress"      % "1.23.0"     % "provided",  // ships with Spark
  "org.apache.kafka"            %  "kafka-clients"         % "3.8.0",
  "org.scalatest"               %% "scalatest"             % "3.2.19"     % Test
)

// Keep third-party Guava/Protobuf from clashing with Hadoop's copies on the YARN classpath, and give
// GeoTrellis its own (newer) Typelevel libraries -- Spark ships older cats-kernel/spire/shapeless.
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**"   -> "smartagri.shaded.guava.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "smartagri.shaded.protobuf.@1").inAll,
  ShadeRule.rename("cats.**"                -> "smartagri.shaded.cats.@1").inAll,
  ShadeRule.rename("spire.**"               -> "smartagri.shaded.spire.@1").inAll,
  ShadeRule.rename("algebra.**"             -> "smartagri.shaded.algebra.@1").inAll,
  ShadeRule.rename("shapeless.**"           -> "smartagri.shaded.shapeless.@1").inAll
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*)          => MergeStrategy.concat
  case PathList("META-INF", "javax.imageio.spi", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)                      => MergeStrategy.discard
  case "reference.conf"                              => MergeStrategy.concat
  case _                                             => MergeStrategy.first
}

Test / fork := true
Test / parallelExecution := false
// Spark on JDK 17 needs these opens in the forked test JVM
Test / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED", "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Xmx2g")
