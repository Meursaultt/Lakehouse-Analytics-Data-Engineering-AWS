#!/usr/bin/env python3
"""
Simulate streaming Olist order events: read olist_orders_dataset.csv and publish JSON to Kafka.
Run from host (localhost:9092) or inside Compose (kafka:29092).
"""

from __future__ import annotations

import csv
import json
import os
import sys
import time

from kafka import KafkaProducer


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC_ORDERS", "olist-orders")
    data_dir = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
    csv_name = os.environ.get("OLIST_ORDERS_CSV", "olist_orders_dataset.csv")
    path = os.path.join(data_dir, csv_name)


    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        linger_ms=5,
    )

    delay = float(os.environ.get("PRODUCER_DELAY_SEC", "0"))

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            payload = {k: (v if v != "" else None) for k, v in row.items()}
            producer.send(topic, value=payload)
            count += 1
            if delay > 0:
                time.sleep(delay)
        producer.flush()

    producer.close()
    print(f"Published {count} order events to {topic} at {bootstrap}")


if __name__ == "__main__":
    main()
