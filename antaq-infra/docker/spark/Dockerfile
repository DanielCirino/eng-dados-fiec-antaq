FROM python:3.11-bullseye as spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl wget \
      vim nano \
      unzip \
      rsync \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

## Download spark and hadoop dependencies and install

# Optional env variables
ENV SPARK_VERSION=3.4.1 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
HADOOP_HOME=/opt/hadoop

RUN cd /tmp && wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
&& mkdir -p ${SPARK_HOME} \
&& mkdir -p ${HADOOP_HOME} \
&& ls -l \
&& tar -xf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/spark --strip-components=1 \
&& rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN export SPARK_HOME=/opt/spark
RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

# Spark libraries

RUN cd /tmp && mkdir -p /tmp/spark_jars && cd /tmp/spark_jars \
  && wget https://repo1.maven.org/maven2/joda-time/joda-time/2.12.2/joda-time-2.12.2.jar \
  && wget https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.2.1/httpclient5-5.2.1.jar \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.400/aws-java-sdk-s3-1.12.400.jar \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-kms/1.12.400/aws-java-sdk-kms-1.12.400.jar \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.400/aws-java-sdk-dynamodb-1.12.400.jar \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.400/aws-java-sdk-core-1.12.400.jar \
  && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.400/aws-java-sdk-1.12.400.jar \
  && wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  && wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.6/slf4j-api-2.0.6.jar \
  && wget https://repo1.maven.org/maven2/org/slf4j/slf4j-reload4j/2.0.6/slf4j-reload4j-2.0.6.jar \
  && mv /tmp/spark_jars/* ${SPARK_HOME}/jars

WORKDIR ${SPARK_HOME}

FROM spark-base as pyspark

# Install python deps
COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]