"""
Gold layer: Silver Delta → aggregated Delta tables for analytics.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F


def _spark_session(app_name: str) -> SparkSession:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")

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


def _silver_path(bucket: str, name: str) -> str:
    return f"s3a://{bucket}/silver/{name}"


def _gold_path(bucket: str, name: str) -> str:
    return f"s3a://{bucket}/gold/{name}"


def main() -> None:
    spark = _spark_session("olist_gold")
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")

    orders = spark.read.format("delta").load(_silver_path(bucket, "silver_orders"))
    items = spark.read.format("delta").load(_silver_path(bucket, "silver_order_items"))
    customers = spark.read.format("delta").load(_silver_path(bucket, "silver_customers"))
    reviews = spark.read.format("delta").load(_silver_path(bucket, "silver_order_reviews"))

    oi = items.alias("i").join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "inner")

    # daily_revenue: date, total_revenue, order_count
    daily = (
        oi.groupBy(F.to_date(F.col("o.order_purchase_timestamp")).alias("date"))
        .agg(
            F.sum(F.col("i.price")).alias("total_revenue"),
            F.countDistinct(F.col("i.order_id")).alias("order_count"),
        )
        .orderBy("date")
    )
    daily.write.format("delta").mode("overwrite").save(_gold_path(bucket, "daily_revenue"))

    # top_sellers: seller_id, total_sales, avg_review_score
    ir = items.alias("i").join(reviews.alias("r"), F.col("i.order_id") == F.col("r.order_id"), "left")
    top_sellers = (
        ir.groupBy(F.col("i.seller_id").alias("seller_id"))
        .agg(
            F.sum(F.col("i.price")).alias("total_sales"),
            F.avg(F.col("r.review_score").cast("double")).alias("avg_review_score"),
        )
        .orderBy(F.desc("total_sales"))
    )
    top_sellers.write.format("delta").mode("overwrite").save(_gold_path(bucket, "top_sellers"))

    # customer_segments: customer_state, total_orders, total_spend
    oc = (
        orders.alias("o")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "inner")
        .join(items.alias("i"), F.col("o.order_id") == F.col("i.order_id"), "inner")
    )
    segments = (
        oc.groupBy(F.col("c.customer_state").alias("customer_state"))
        .agg(
            F.countDistinct(F.col("o.order_id")).alias("total_orders"),
            F.sum(F.col("i.price")).alias("total_spend"),
        )
        .orderBy(F.desc("total_spend"))
    )
    segments.write.format("delta").mode("overwrite").save(_gold_path(bucket, "customer_segments"))

    spark.stop()


if __name__ == "__main__":
    main()
