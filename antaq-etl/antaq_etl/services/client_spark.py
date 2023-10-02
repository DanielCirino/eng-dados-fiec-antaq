import os
import sys
import pyspark

from pyspark.sql import SparkSession
from airflow.models import Variable
import logging

PYTHON_LOCATION = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION
logging.info(f"PYTHON_LOCATION--->{PYTHON_LOCATION}")


def obterJars():
    dir = "/opt/airflow/spark-jars"
    jars = os.listdir(dir)
    stringJars = ""

    for jar in jars:
        logging.info(f"/opt/airflow/spark-jars/{jar}")
        stringJars += f"/opt/airflow/spark-jars/{jar},"

    return stringJars[:-1]


def obterSparkClient(appName="default"):
    conf = pyspark.SparkConf()
    conf.setAppName(f"snif-florestal-{appName}")
    conf.setMaster("spark://spark-master:7077")

    conf.set("spark.hadoop.fs.s3a.endpoint", Variable.get("AWS_ENDPOINT")) \
        .set("spark.hadoop.fs.s3a.endpoint.region", Variable.get("AWS_REGION")) \
        .set("spark.hadoop.fs.s3a.access.key", Variable.get("AWS_ACCESS_KEY_ID")) \
        .set("spark.hadoop.fs.s3a.secret.key", Variable.get("AWS_SECRET_ACCESS_KEY")) \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.com.amazonaws.services.s3.enableV2", "true") \
        .set("spark.jars", obterJars()) \
        .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .set("spark.hadoop.fs.s3a.fast.upload", "true") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.sql.sources.commitProtocolClass",
             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g")
    # .set("spark.cores.max", "16")
    # .set("parquet.summary.metadata.level", "false")
    # .set("spark.hadoop.fs.s3a.committer.name", "magic") \
    # .set("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")

    sc = pyspark.SparkContext(conf=conf).getOrCreate()

    return SparkSession(sc)


def salvarDataFrameBancoDados(df, bancoDados, tabela: str):
    df.write.format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres-server:5432/{bancoDados}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", tabela) \
        .option("user", "postgres").option("password", "postgres").save(mode="overwrite")

    logging.info(f"{df.count()} registros salvos na tabela {tabela}]")


def getDataframeFromSql(sparkSession: SparkSession, bancoDados: str, sql: str):

    df = sparkSession.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://postgres-server:5432/{bancoDados}") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("query", sql) \
        .load()

    return df