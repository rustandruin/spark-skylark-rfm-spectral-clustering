name := "spectralclusteringexample"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.1" % "provided"

//libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

//libraryDependencies += "io.spray" %%  "spray-json" % "1.3.2"

lazy val runTest = taskKey[Unit]("Submit test of spectral clustering")
runTest <<= (assembly in Compile) map {
  (jarFile: File) => s"/opt/Spark/bin/spark-submit --verbose --master yarn-cluster --driver-memory 1G --class org.apache.spark.mllib.linalg.distributed.SpectralClusterExample ${jarFile} file://./data/test.csv 55 200 3 100 2>&1 | tee data/run.log" !
}
