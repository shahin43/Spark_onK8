FROM cluster-base

# -- Layer: Apache Spark

ARG spark_version=3.0.2
ARG hadoop_version=3.2



RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    apt-get install -y nano && \
    rm spark.tgz


#COPY dockerImages/required_jars/hadoop-aws-2.7.4.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/hadoop-aws-2.7.4.jar
#COPY dockerImages/required_jars/hadoop-common-2.7.4.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/hadoop-common-2.7.4.jar
#COPY dockerImages/required_jars/aws-java-sdk-1.7.4.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/aws-java-sdk-1.7.4.jar

COPY ./docker/spark/required_jars/hadoop-aws-3.2.0.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/hadoop-aws-3.2.0.jar
COPY ./docker/spark/required_jars/hadoop-common-3.2.0.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/hadoop-common-3.2.0.jar
COPY ./docker/spark/required_jars/aws-java-sdk-1.11.30.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/aws-java-sdk-1.11.30.jar
#COPY dockerImages/required_jars/aws-java-sdk-bundle-1.11.874.jar /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/aws-java-sdk-bundle-1.11.874.jar


ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3


# -- Runtime
WORKDIR ${SPARK_HOME}