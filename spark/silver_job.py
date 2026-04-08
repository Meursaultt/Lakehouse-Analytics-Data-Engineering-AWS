"""
Silver layer: Bronze Parquet → cleaned Delta tables + data quality report.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType


def _spark_session(app_name: str) -> SparkSession:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", key)
        .config("spark.hadoop.fs.s3a.secret.key", secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def _bronze_path(bucket: str, name: str) -> str:
    return f"s3a://{bucket}/bronze/{name}"


def _silver_path(bucket: str, name: str) -> str:
    return f"s3a://{bucket}/silver/{name}"


def _parse_ts(c):
    return F.to_timestamp(F.regexp_replace(F.trim(F.col(c).cast("string")), "/", "-"), "yyyy-MM-dd HH:mm:ss")


def main() -> None:
    spark = _spark_session("olist_silver")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    orders = spark.read.option("mergeSchema", "true").parquet(_bronze_path(bucket, "olist_orders"))
    items = spark.read.option("mergeSchema", "true").parquet(_bronze_path(bucket, "olist_order_items"))
    customers = spark.read.option("mergeSchema", "true").parquet(_bronze_path(bucket, "olist_customers"))
    reviews = spark.read.option("mergeSchema", "true").parquet(_bronze_path(bucket, "olist_order_reviews"))

    # --- silver_orders
    so = (
        orders.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            _parse_ts("order_purchase_timestamp").alias("order_purchase_timestamp"),
            _parse_ts("order_approved_at").alias("order_approved_at"),
            _parse_ts("order_delivered_carrier_date").alias("order_delivered_carrier_date"),
            _parse_ts("order_delivered_customer_date").alias("order_delivered_customer_date"),
            _parse_ts("order_estimated_delivery_date").alias("order_estimated_delivery_date"),
        )
        .filter(F.col("order_id").isNotNull() & (F.length(F.col("order_id")) > 0))
        .dropDuplicates(["order_id"])
    )

    # --- silver_order_items
    si = (
        items.select(
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("order_item_id").cast(IntegerType()).alias("order_item_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("seller_id")).alias("seller_id"),
            _parse_ts("shipping_limit_date").alias("shipping_limit_date"),
            F.col("price").cast(DoubleType()).alias("price"),
            F.col("freight_value").cast(DoubleType()).alias("freight_value"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["order_id", "order_item_id"])
    )

    # --- silver_customers
    sc = (
        customers.select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("customer_unique_id")).alias("customer_unique_id"),
            F.col("customer_zip_code_prefix").cast(StringType()).alias("customer_zip_code_prefix"),
            F.trim(F.col("customer_city")).alias("customer_city"),
            F.upper(F.trim(F.col("customer_state"))).alias("customer_state"),
        )
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    # --- silver_order_reviews
    sr = (
        reviews.select(
            F.trim(F.col("review_id")).alias("review_id"),
            F.trim(F.col("order_id")).alias("order_id"),
            F.col("review_score").cast(IntegerType()).alias("review_score"),
            _parse_ts("review_creation_date").alias("review_creation_date"),
            _parse_ts("review_answer_timestamp").alias("review_answer_timestamp"),
        )
        .filter(F.col("order_id").isNotNull())
        .dropDuplicates(["review_id"])
    )

    so.write.format("delta").mode("overwrite").save(_silver_path(bucket, "silver_orders"))
    si.write.format("delta").mode("overwrite").save(_silver_path(bucket, "silver_order_items"))
    sc.write.format("delta").mode("overwrite").save(_silver_path(bucket, "silver_customers"))
    sr.write.format("delta").mode("overwrite").save(_silver_path(bucket, "silver_order_reviews"))

    # --- Data quality checks (on final silver_orders)
    total_orders = so.count()
    distinct_order_ids = so.select("order_id").distinct().count()
    order_id_uniqueness_ok = total_orders > 0 and distinct_order_ids == total_orders

    def null_rate(df, col_name: str) -> float:
        c = df.filter(F.col(col_name).isNull()).count()
        n = df.count()
        return float(c) / float(n) if n else 1.0

    nr_order_id = null_rate(so, "order_id")
    nr_customer_id = null_rate(so, "customer_id")

    checks = [
        ("row_count_orders_gt_0", total_orders > 0, float(total_orders), "count"),
        ("null_rate_order_id_lt_5pct", nr_order_id < 0.05, nr_order_id, "rate"),
        ("null_rate_customer_id_lt_5pct", nr_customer_id < 0.05, nr_customer_id, "rate"),
        (
            "no_duplicate_order_ids",
            order_id_uniqueness_ok,
            float(total_orders - distinct_order_ids),
            "non_unique_rows",
        ),
    ]

    report_rows = [
        (
            name,
            bool(ok),
            float(val),
            metric_kind,
            run_id,
        )
        for name, ok, val, metric_kind in checks
    ]

    report_df = spark.createDataFrame(
        report_rows,
        schema="check_name string, passed boolean, metric_value double, metric_kind string, run_timestamp string",
    )
    qpath = _silver_path(bucket, "quality_report")
    report_df.write.format("delta").mode("append").save(qpath)

    failed = [c for c in checks if not c[1]]
    if failed:
        spark.stop()
        raise RuntimeError(f"Data quality failed: {[f[0] for f in failed]}")

    spark.stop()


if __name__ == "__main__":
    main()
