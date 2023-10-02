import os
import sys
import pyspark

from pyspark.sql import SparkSession
from airflow.models import Variable
import logging

PYTHON_LOCATION = sys.executable
os.environ["PYSPARK_PYTHON"] = PYTHON_LOCATION
logging.info(f"PYTHON_LOCATION--->{PYTHON_LOCATION}")

AWS_ENDPOINT = Variable.get("AWS_ENDPOINT", os.getenv("S3_ENDPOINT_URL", ""))
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID", os.getenv("S3_AWS_ACCESS_KEY_ID", ""))
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY", os.getenv("S3_AWS_SECRET_ACCESS_KEY", ""))
AWS_REGION = Variable.get("AWS_REGION", os.getenv("S3_AWS_REGION_NAME", ""))

SPARK_MASTER_URL = Variable.get("SPARK_MASTER_URL", os.getenv("SPARK_MASTER_URL", ""))
SPARK_JARS_DIR = Variable.get("SPARK_JARS_DIR", os.getenv("SPARK_JARS_DIR", ""))

MSSQL_HOST = Variable.get("MSSQL_HOST", os.getenv("MSSQL_HOST", ""))
MSSQL_PORT = Variable.get("MSSQL_PORT", os.getenv("MSSQL_PORT", ""))
MSSQL_USER = Variable.get("MSSQL_USER", os.getenv("MSSQL_USER", ""))
MSSQL_PWD = Variable.get("MSSQL_PWD", os.getenv("MSSQL_PWD", ""))


def obterJars():
    jars = os.listdir(SPARK_JARS_DIR)
    stringJars = ""
    logging.info(f"Arquivos .jar na pasta {dir}: {len(jars)}")
    for jar in jars:
        logging.info(f"/opt/airflow/spark-jars/{jar}")
        stringJars += f"/opt/airflow/spark-jars/{jar},"

    return stringJars[:-1]


def obterSparkClient(appName="default"):
    logging.info(f"---------------->{SPARK_MASTER_URL}")

    conf = pyspark.SparkConf()
    conf.setAppName(f"fiec-antaq-{appName}")
    conf.setMaster(SPARK_MASTER_URL)

    conf.set("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT) \
        .set("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION) \
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", False) \
        .set("spark.hadoop.com.amazonaws.services.s3.enableV2", True) \
        .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace") \
        .set("spark.hadoop.fs.s3a.fast.upload", True) \
        .set("spark.hadoop.fs.s3a.path.style.access", True) \
        .set("spark.sql.sources.commitProtocolClass",
             "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .set("spark.jars", obterJars()) \
        .set("spark.executor.memory", "3g") \
        .set("spark.driver.memory", "3g") \
        .set("spark.cores.max", "16") \
        # .set("spark.hadoop.fs.s3a.committer.name", "magic") \
    # .set("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")

    sc = pyspark.SparkContext(conf=conf).getOrCreate()
    sc.setLogLevel("WARN")

    return SparkSession(sc)


def salvarDataFrameBancoDados(df, bancoDados, tabela: str, modoEscrita="overwrite"):
    df.write \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url", f"jdbc:sqlserver://{MSSQL_HOST}:{MSSQL_PORT};databaseName={bancoDados};") \
        .option("dbtable", tabela) \
        .option("user", MSSQL_USER) \
        .option("password", MSSQL_PWD) \
        .option("encrypt", False) \
        .mode(modoEscrita) \
        .save()

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
