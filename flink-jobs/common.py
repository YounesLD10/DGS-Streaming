"""
Shared utilities for all HPS pipeline jobs.
"""
import io
import json
import logging
import os
import re
import socket
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from minio import Minio

# When connecting via port-forward, Kafka advertises its internal pod DNS name
# (*.kafka-brokers.kafka.svc) which isn't resolvable from the host.
# Redirect those hostnames to 127.0.0.1 so the port-forward on :9092 is used.
_real_getaddrinfo = socket.getaddrinfo
def _patched_getaddrinfo(host, port, *args, **kwargs):
    if isinstance(host, str) and "kafka-brokers.kafka.svc" in host:
        host = "127.0.0.1"
    return _real_getaddrinfo(host, port, *args, **kwargs)
socket.getaddrinfo = _patched_getaddrinfo

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",     "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",   "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",   "admin123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",       "rt-payments")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE",     "10"))

DATE_FORMATS = ["%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"]

DATE_FIELDS = [
    "TRANSACTION_LOCAL_DATE",
    "TRANSMISSION_DATE_AND_TIME",
    "RESPONSE_DATE_AND_TIME",
    "CAPTURE_DATE",
    "BUSINESS_DATE",
    "SETTLEMENT_DATE",
    "CONVERSION_DATE",
]

AMOUNT_FIELDS = [
    "TRANSACTION_AMOUNT",
    "BILLING_AMOUNT",
    "SETTLEMENT_AMOUNT",
    "ORIGINAL_AMOUNT",
    "FEE_AMOUNT",
]


def get_minio() -> Minio:
    host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(host, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def get_consumer(topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=30000,
    )


def flush_to_minio(client: Minio, records: list, prefix: str, log: logging.Logger) -> str:
    if not records:
        return ""
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    key = f"{prefix}/{ts}_{uid}.json"
    payload = ("\n".join(json.dumps(r) for r in records)).encode("utf-8")
    client.put_object(
        MINIO_BUCKET, key,
        io.BytesIO(payload), len(payload),
        content_type="application/json",
    )
    log.info(f"MinIO ← {len(records)} records → {MINIO_BUCKET}/{key}")
    return key


def parse_date(value: str):
    if not value or not str(value).strip():
        return None
    raw = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    return raw


def parse_amount(value) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except (ValueError, TypeError):
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
