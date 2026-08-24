#!/usr/bin/env python3
"""
hps.public.transactions (Debezium CDC, plaintext flat rows) -> payments
(Fernet-encrypted envelope expected by job1_decrypt.py)
==========================================================================
Bridges the existing debezium-hps-source connector's CDC output into the
real ingestion topic Job1 already consumes, so transactions entering via
direct database writes to postgres-hps.transactions flow through the
exact same Job1-4 -> gold-sink pipeline as the CSV producer.

Debezium's `transforms.unwrap` (ExtractNewRecordState) already flattens
each change event to the row's new-state column values as plain JSON
(lowercase column names, no schema/before/after wrapper). This bridge:
  1. Re-maps the lowercase source-table columns to the uppercase field
     names every downstream job expects (job2_validate.py etc. all read
     tx.get("MESSAGE_TYPE"), tx.get("AUTHORIZATION_CODE"), ...).
  2. Encrypts the remapped row with Fernet (the SAME key Job1 uses).
  3. Wraps it in the exact envelope job1_decrypt.py parses:
     {"eventId", "table", "operation", "payload": <fernet token>, "timestamp"}
  4. Publishes to the real "payments" topic.

IMPORTANT — known field coverage gap (see scripts/README or PR notes):
postgres-hps.transactions has only 9 columns (authorization_code,
message_type, product_code, transaction_amount, transaction_currency,
issuing_bank, card_type, matching_status, reject_code) versus 30+ fields
in the CSV producer's source data (CARD_NUMBER, all settlement/date
fields, MCC, etc.). Records bridged through this path will normalize and
score correctly (job2/job3/job4 tolerate missing optional fields), but
will carry far less enrichment than CSV-sourced records. This is a
structural source-schema limitation, not a bug in this bridge.
"""
import json
import logging
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone

from cryptography.fernet import Fernet
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# Host-only: when run from outside the cluster (e.g. via kubectl port-forward
# for the standalone proof in this PR), the broker advertises its internal
# pod DNS name which the host can't resolve. Redirect it to localhost, same
# pattern as scripts/producer.py. If this script is ever deployed in-cluster
# (as a long-lived Deployment, like gold_flattener.py), REMOVE this patch —
# in-cluster DNS resolves these hostnames correctly on its own, and this
# patch would incorrectly redirect them to a nonexistent local port.
_orig_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, *args, **kwargs):
    if isinstance(host, str) and "kafka-brokers.kafka.svc" in host:
        host = "127.0.0.1"
    return _orig_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _patched_getaddrinfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [cdc-bridge] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092")
SOURCE_TOPIC    = os.getenv("SOURCE_TOPIC", "hps.public.transactions")
TARGET_TOPIC    = os.getenv("TARGET_TOPIC", "payments")
GROUP_ID        = os.getenv("GROUP_ID", "cdc-bridge")

FERNET_KEY = os.getenv("FERNET_KEY")
if not FERNET_KEY:
    log.error("FERNET_KEY env var is required (must match the Flink jobs' key) — aborting")
    sys.exit(1)

# lowercase source column -> uppercase pipeline field name
_FIELD_MAP = {
    "authorization_code":  "AUTHORIZATION_CODE",
    "message_type":        "MESSAGE_TYPE",
    "product_code":        "PRODUCT_CODE",
    "transaction_amount":  "TRANSACTION_AMOUNT",
    "transaction_currency": "TRANSACTION_CURRENCY",
    "issuing_bank":         "ISSUING_BANK",
    "card_type":            "CARD_TYPE",
    "matching_status":      "MATCHING_STATUS",
    "reject_code":          "REJECT_CODE",
}


def remap(row: dict) -> dict:
    """Translate Debezium's flat lowercase row into pipeline field names."""
    out = {}
    for src, dst in _FIELD_MAP.items():
        if src in row and row[src] is not None:
            out[dst] = row[src]
    return out


def wait_for_kafka(bootstrap: str, retries: int = 20):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                SOURCE_TOPIC,
                bootstrap_servers=[bootstrap],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
                consumer_timeout_ms=10000,
            )
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Kafka connected (attempt %d)", attempt)
            return consumer, producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retry %d/%d ...", attempt, retries)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after %d retries" % retries)


def main():
    fernet = Fernet(FERNET_KEY.encode())
    consumer, producer = wait_for_kafka(KAFKA_BOOTSTRAP)
    log.info("Bridging %s -> %s", SOURCE_TOPIC, TARGET_TOPIC)

    bridged = skipped = 0
    for msg in consumer:
        if msg.value is None:
            skipped += 1
            continue
        row = remap(msg.value)
        if not row.get("AUTHORIZATION_CODE"):
            skipped += 1
            continue

        encrypted = fernet.encrypt(json.dumps(row).encode("utf-8")).decode("utf-8")
        envelope = {
            "eventId":   str(uuid.uuid4()),
            "table":     "postgres_hps_transactions",
            "operation": "INSERT",
            "payload":   encrypted,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TARGET_TOPIC, value=envelope)
        bridged += 1
        log.info("Bridged AUTHORIZATION_CODE=%s (offset=%d)", row.get("AUTHORIZATION_CODE"), msg.offset)

    producer.flush()
    log.info("Done. bridged=%d skipped=%d", bridged, skipped)


if __name__ == "__main__":
    main()
