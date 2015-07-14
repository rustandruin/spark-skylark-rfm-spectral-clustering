/opt/Spark/bin/spark-submit --verbose --master yarn-cluster \
	--driver-memory 4G \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=data \
	--jars $1 \
	--class org.apache.spark.mllib.linalg.distributed.SpectralClusterExample $1 \
	hdfs:///user/ubuntu/dataset 131048 8258911 10 100 \
	2>&1 | tee data/run.log
#  (jarFile: File) => s"/opt/Spark/bin/spark-submit --verbose --master yarn-cluster --driver-memory 1G --class org.apache.spark.mllib.linalg.distributed.SpectralClusterExample ${jarFile} file://./data/test.csv 55 200 3 100 2>&1 | tee data/run.log" !
