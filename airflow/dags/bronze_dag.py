"""
Bronze: Kafka → raw Parquet (orders) + static Olist CSVs → Parquet (reference tables).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from include.bronze_helpers import consume_kafka_orders_to_bronze, land_static_csv_to_bronze

DEFAULT_ARGS = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
}

def _kafka_to_bronze():
    out = consume_kafka_orders_to_bronze()
    if out.get("rows_written", 0) == 0:
        raise ValueError(
            "No Kafka messages consumed. Start the producer: "
        )


def _static_to_bronze():
    land_static_csv_to_bronze()


with DAG(
    dag_id="bronze_olist_lakehouse",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "olist", "kafka"],
) as dag:
    t_kafka = PythonOperator(
        task_id="kafka_orders_to_minio_parquet",
        python_callable=_kafka_to_bronze,
    )
    t_static = PythonOperator(
        task_id="static_csv_to_minio_parquet",
        python_callable=_static_to_bronze,
    )
