FROM apache/airflow:2.6.3-python3.9

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless wget nano procps\
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.4.1

USER airflow

# Spark libraries

RUN cd /tmp && mkdir -p /tmp/spark-jars && cd /tmp/spark-jars \
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
  && wget https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.13/3.3.1/spark-hadoop-cloud_2.13-3.3.1.jar\
  && wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar \
  && mkdir -p /opt/airflow/spark-jars \
  && mv /tmp/spark-jars/* /opt/airflow/spark-jars

RUN pip install --no-cache-dir "apache-airflow[amazon]==${AIRFLOW_VERSION}" python-dotenv boto3 pyspark==$SPARK_VERSION

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME



