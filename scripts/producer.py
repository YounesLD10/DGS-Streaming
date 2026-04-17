#!/usr/bin/env python3
"""
HPS Real-Time PoC — Kafka producer
Reads a Kaggle credit-card CSV, encrypts each row with Fernet,
wraps it in a CDC envelope, and publishes to a Kafka topic.
"""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from cryptography.fernet import Fernet
from kafka import KafkaProducer
from kafka.errors import KafkaError


def parse_args():
    p = argparse.ArgumentParser(description="HPS payments Kafka producer")
    p.add_argument("--csv",       required=True,             help="Path to creditcard CSV file")
    p.add_argument("--bootstrap", default="localhost:9094",  help="Kafka bootstrap server")
    p.add_argument("--topic",     default="payments",        help="Target Kafka topic")
    p.add_argument("--rate",      type=float, default=10.0,  help="Messages per second")
    p.add_argument("--limit",     type=int,   default=0,     help="Max rows to send (0 = all)")
    return p.parse_args()


def make_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=5,
    )


def main():
    args = parse_args()

    key = Fernet.generate_key()
    fernet = Fernet(key)
    print(f"[producer] Fernet key (save to decrypt): {key.decode()}")

    print(f"[producer] Loading CSV: {args.csv}")
    df = pd.read_csv(args.csv)
    if args.limit > 0:
        df = df.head(args.limit)
    total = len(df)
    print(f"[producer] Rows to send: {total}  rate: {args.rate} msg/s  topic: {args.topic}")

    producer = make_producer(args.bootstrap)
    interval = 1.0 / args.rate
    sent = 0
    errors = 0

    for _, row in df.iterrows():
        payload_bytes = row.to_json().encode("utf-8")
        encrypted = fernet.encrypt(payload_bytes).decode("utf-8")

        envelope = {
            "eventId":   str(uuid.uuid4()),
            "table":     "creditcard_transactions",
            "operation": "INSERT",
            "payload":   encrypted,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            producer.send(args.topic, value=envelope)
            sent += 1
            if sent % 100 == 0:
                print(f"[producer] Sent {sent}/{total}")
        except KafkaError as exc:
            errors += 1
            print(f"[producer] ERROR: {exc}")

        time.sleep(interval)

    producer.flush()
    print(f"[producer] Done. sent={sent}  errors={errors}")


if __name__ == "__main__":
    main()
