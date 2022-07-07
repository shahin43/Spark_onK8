# -- Software Stack Version

SPARK_VERSION="3.0.2"
HADOOP_VERSION="3.2"

# -- Building the Images - Spark cluster
docker build \
  -f docker/spark/sparkcluster_baseImage.Dockerfile \
  -t cluster-base .


# spark base image with version and HDFS
docker build \
  --build-arg spark_version="${SPARK_VERSION}" \
  --build-arg hadoop_version="${HADOOP_VERSION}" \
  -f docker/spark/sparkbase.Dockerfile \
  -t spark-base .

# spark master image
docker build \
  -f docker/spark/spark-master.Dockerfile \
  -t spark-master .


# spark worker image
docker build \
  -f docker/spark/spark-worker.Dockerfile \
  -t spark-worker .