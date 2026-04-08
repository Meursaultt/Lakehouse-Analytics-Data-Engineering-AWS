"""
Gold: PySpark job — Silver Delta → aggregated Gold Delta tables.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
}

SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.0.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SPARK_CONF = {
    "spark.driver.memory": "2g",
    "spark.executor.memory": "2g",
    "spark.sql.adaptive.enabled": "true",
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

ENV_VARS = {
    "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    "MINIO_BUCKET": os.environ.get("MINIO_BUCKET", "lakehouse"),
}

with DAG(
    dag_id="gold_olist_lakehouse",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gold", "olist", "spark", "delta"],
) as dag:
    SparkSubmitOperator(
        task_id="pyspark_gold_delta",
        application="/opt/airflow/spark/gold_job.py",
        conn_id="spark_default",
        deploy_mode="client",
        name="olist_gold",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        env_vars=ENV_VARS,
        verbose=False,
    )
