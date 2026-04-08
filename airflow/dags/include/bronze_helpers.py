"""Bronze layer: Kafka consumption and static CSV landing to MinIO (Parquet)."""

from __future__ import annotations

import io
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
from kafka import KafkaConsumer


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def consume_kafka_orders_to_bronze(
    consumer_timeout_ms: int = 120_000,
) -> dict[str, Any]:
    """
    Consume JSON order events from Kafka (same topic as kafka/producer.py: producer.send).
    Writes one Parquet file under bronze/olist_orders/dt=.../ — matches Silver read path.
    """
    bucket = os.environ["MINIO_BUCKET"]
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ.get("KAFKA_TOPIC_ORDERS", "olist-orders")
    client = _s3_client()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="airflow-bronze-consumer",
        consumer_timeout_ms=consumer_timeout_ms,
    )

    rows: list[dict] = []
    try:
        for msg in consumer:
            if msg.value:
                rows.append(msg.value)
    finally:
        consumer.close()

    if not rows:
        return {"status": "no_messages", "rows_written": 0, "s3_keys": []}

    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"bronze/olist_orders/dt={dt}/part-{uuid.uuid4().hex}.parquet"
    client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")

    return {"status": "ok", "rows_written": len(rows), "s3_keys": [key]}


# Kaggle Olist bundle: CSV filename -> bronze folder under s3://.../bronze/<prefix>/...
# olist_orders_dataset.csv is NOT listed — orders are simulated via Kafka -> bronze/olist_orders/.
STATIC_CSV_MAP = {
    "olist_order_items_dataset.csv": "olist_order_items",
    "olist_order_payments_dataset.csv": "olist_order_payments",
    "olist_customers_dataset.csv": "olist_customers",
    "olist_order_reviews_dataset.csv": "olist_order_reviews",
    "olist_sellers_dataset.csv": "olist_sellers",
    "olist_products_dataset.csv": "olist_products",
    "olist_geolocation_dataset.csv": "olist_geolocation",
    "product_category_name_translation.csv": "product_category_name_translation",
}


def land_static_csv_to_bronze(data_dir: str | None = None) -> dict[str, Any]:
    """Land Olist CSVs from disk to MinIO as Parquet (bronze). Missing files are skipped; orders use Kafka."""
    base = data_dir or os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data")
    bucket = os.environ["MINIO_BUCKET"]
    client = _s3_client()
    results: list[dict] = []

    for filename, prefix in STATIC_CSV_MAP.items():
        path = os.path.join(base, filename)
        if not os.path.isfile(path):
            results.append({"file": filename, "status": "skipped_missing"})
            continue
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"bronze/{prefix}/dt={dt}/{filename.replace('.csv', '')}.parquet"
        client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(), ContentType="application/octet-stream")
        results.append({"file": filename, "status": "ok", "rows": len(df), "key": key})

    return {"landed": results}
