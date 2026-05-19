#!/usr/bin/env python3
"""
RT-Payments Producer
Reads the Powercard Operations CSV (ISO 8583 records), wraps each row in an
event envelope, encrypts the payload with Fernet, and publishes to Kafka.

Usage:
  export FERNET_KEY=$(kubectl get secret -n ingestion fernet-key -o jsonpath='{.data.key}' | base64 -d)
  python producer.py --csv data.csv --bootstrap <minikube-ip>:<nodeport> [--rate 50] [--limit 1000]
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "flink-jobs"))
from common.crypto import encrypt_dict  # noqa: E402

TOPIC = "payments"


def build_envelope(row: dict, idx: int) -> dict:
    return {
        "eventId": f"evt-{idx:08d}",
        "schemaVersion": "1.0",
        "source": "powercard-csv",
        "encrypted_payload": encrypt_dict(row),
        "producedAt": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="RT-Payments Kafka Producer")
    ap.add_argument("--csv", required=True, help="Path to the Powercard CSV")
    ap.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap (host:port)")
    ap.add_argument("--topic", default=TOPIC, help="Target Kafka topic")
    ap.add_argument("--rate", type=float, default=20.0, help="Messages per second (0 = no throttle)")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to send (0 = all)")
    args = ap.parse_args()

    print(f"[producer] reading {args.csv}", flush=True)
    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False, na_values=[""])
    if args.limit > 0:
        df = df.head(args.limit)
    total = len(df)
    print(f"[producer] {total} rows loaded → topic={args.topic} broker={args.bootstrap}", flush=True)

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=5,
            linger_ms=20,
            compression_type="lz4",
        )
    except NoBrokersAvailable:
        print(f"[producer] cannot reach broker at {args.bootstrap}", file=sys.stderr)
        return 2

    interval = 1.0 / args.rate if args.rate > 0 else 0.0
    sent = 0
    start = time.time()
    for i, row in df.iterrows():
        env = build_envelope(row.where(pd.notna(row), None).to_dict(), i)
        producer.send(args.topic, key=env["eventId"], value=env)
        sent += 1
        if sent % 100 == 0:
            elapsed = time.time() - start
            rps = sent / elapsed if elapsed > 0 else 0
            print(f"[producer] {sent}/{total} sent ({rps:.1f} msg/s)", flush=True)
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    elapsed = time.time() - start
    print(f"[producer] done — {sent} messages in {elapsed:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
