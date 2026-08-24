#!/usr/bin/env python3
"""Expose SWAM business metrics for Kafka, MinIO, and PostgreSQL.

The exporter deliberately discovers MinIO credentials and the PostgreSQL gold
source at runtime so that it remains valid across local Minikube deployments.
"""

import base64
import logging
import os
import shlex
import socket
import subprocess
import time
from threading import Thread

import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer, TopicPartition
from minio import Minio
from prometheus_client import Gauge, start_http_server


# Configuration
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "8888"))
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL_SECONDS", "15"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "").strip()
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "rt-payments")
MINIO_SECRET_NAMESPACE = os.getenv("MINIO_SECRET_NAMESPACE", "minio")
MINIO_SECRET_NAME = os.getenv("MINIO_SECRET_NAME", "minio")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() in {"1", "true", "yes"}
KUBECTL_COMMAND = shlex.split(os.getenv("KUBECTL_COMMAND", "minikube kubectl --"))
PG_DSN = os.getenv("PG_DSN", "postgresql://hps:hps123@localhost:5432/datamart")

# The single-broker Kafka cluster advertises internal pod DNS names. When the
# exporter runs on the host, resolve those names through the local port-forward.
_KAFKA_LOCAL_PORT = int(os.getenv("KAFKA_LOCAL_PORT", "9094"))
_ORIGINAL_GETADDRINFO = socket.getaddrinfo


def _patched_getaddrinfo(host, port, *args, **kwargs):
    if isinstance(host, str) and host.endswith(".kafka.svc"):
        return _ORIGINAL_GETADDRINFO("127.0.0.1", _KAFKA_LOCAL_PORT, *args, **kwargs)
    return _ORIGINAL_GETADDRINFO(host, port, *args, **kwargs)


socket.getaddrinfo = _patched_getaddrinfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [hps-exporter] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)
# kafka-python emits connection internals at INFO; exporter state transitions
# below provide the useful operational signal without per-refresh noise.
logging.getLogger("kafka").setLevel(logging.WARNING)

STAGE_TOPICS = {
    "raw_encrypted": "payments",
    "decrypted": "payments.decrypted",
    "validated": "payments.validated",
    "normalized": "payments.normalized",
    "gold_enriched": "payments.gold",
    "dead_letter": "payments.dlq",
}
LAYER_NAMES = ("bronze", "silver", "gold")
RISK_COLUMN_ALIASES = {"riskscore", "risk", "risklevel", "riskrating", "scorerisque", "niveaurisque"}
CHANNEL_COLUMN_ALIASES = {
    "paymentchannel",
    "channel",
    "paymentmethod",
    "canal",
    "canalpayment",
    "canalpaiement",
}


swam_payments_total = Gauge(
    "swam_payments_total", "Total messages observed per pipeline stage (Kafka topic end-offset)", ["stage"]
)
swam_minio_objects = Gauge("swam_minio_objects", "Object count per discovered MinIO medallion layer", ["layer"])
# Kept for existing dashboards; these mirror the discovered live gold table.
swam_datamart_total = Gauge("swam_datamart_total", "Total rows in the discovered PostgreSQL gold table")
swam_risk_score_total = Gauge("swam_risk_score_total", "Gold rows grouped by the discovered risk column", ["risk"])
swam_payment_channel_total = Gauge(
    "swam_payment_channel_total", "Gold rows grouped by the discovered payment channel column", ["channel"]
)
swam_gold_transactions_total = Gauge("swam_gold_transactions_total", "Total rows in the discovered PostgreSQL gold table")
swam_gold_risk_score_total = Gauge(
    "swam_gold_risk_score_total", "Discovered gold table rows grouped by risk", ["risk"]
)
swam_gold_payment_channel_total = Gauge(
    "swam_gold_payment_channel_total", "Discovered gold table rows grouped by payment channel", ["channel"]
)

_source_available = {}
_resolved_minio_credentials = None


def _report_refresh(source, error=None):
    """Log availability transitions while suppressing repeated failures."""
    available = error is None
    previous = _source_available.get(source)
    _source_available[source] = available
    display_name = {"kafka": "Kafka", "minio": "MinIO", "postgres": "PostgreSQL"}[source]
    if available and previous is not True:
        log.info("%s metrics refreshed", display_name)
    elif not available and previous is not False:
        log.warning("%s unavailable: %s", display_name, error)


def _secret_value(key):
    """Return one base64-encoded value from the configured Kubernetes Secret."""
    command = KUBECTL_COMMAND + [
        "get", "secret", MINIO_SECRET_NAME, "-n", MINIO_SECRET_NAMESPACE,
        "-o", f"jsonpath={{.data.{key}}}",
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=10)
    encoded = result.stdout.strip()
    if not encoded:
        raise RuntimeError(f"Kubernetes Secret {MINIO_SECRET_NAMESPACE}/{MINIO_SECRET_NAME} has no {key} value")
    return base64.b64decode(encoded).decode("utf-8")


def _minio_credentials():
    """Use explicit environment credentials, otherwise load minio/minio once."""
    global _resolved_minio_credentials
    if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
        return MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    if bool(MINIO_ACCESS_KEY) != bool(MINIO_SECRET_KEY):
        raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be provided together")
    if _resolved_minio_credentials is None:
        _resolved_minio_credentials = (_secret_value("rootUser"), _secret_value("rootPassword"))
        log.info("Loaded MinIO credentials from Kubernetes Secret %s/%s", MINIO_SECRET_NAMESPACE, MINIO_SECRET_NAME)
    return _resolved_minio_credentials


def refresh_kafka():
    try:
        consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP])
        try:
            for stage, topic in STAGE_TOPICS.items():
                partitions = consumer.partitions_for_topic(topic)
                if not partitions:
                    log.warning("Kafka topic %s not found; skipping stage=%s", topic, stage)
                    continue
                topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
                swam_payments_total.labels(stage=stage).set(sum(consumer.end_offsets(topic_partitions).values()))
        finally:
            consumer.close()
    except Exception as exc:
        _report_refresh("kafka", exc)
    else:
        _report_refresh("kafka")


def _object_layers(object_name):
    """Infer medallion layers from any directory component in an object key."""
    components = {component.lower() for component in object_name.strip("/").split("/")}
    return [layer for layer in LAYER_NAMES if any(layer in component for component in components)]


def refresh_minio():
    try:
        access_key, secret_key = _minio_credentials()
        client = Minio(MINIO_ENDPOINT, access_key=access_key, secret_key=secret_key, secure=MINIO_SECURE)
        counts = {layer: 0 for layer in LAYER_NAMES}
        discovered_paths = {layer: set() for layer in LAYER_NAMES}
        for item in client.list_objects(MINIO_BUCKET, recursive=True):
            for layer in _object_layers(item.object_name):
                counts[layer] += 1
                discovered_paths[layer].add(item.object_name.split("/", 1)[0])
        for layer, count in counts.items():
            swam_minio_objects.labels(layer=layer).set(count)
            if discovered_paths[layer]:
                log.debug("MinIO %s layer discovered under %s", layer, sorted(discovered_paths[layer]))
    except Exception as exc:
        _report_refresh("minio", exc)
    else:
        _report_refresh("minio")


def _normalise_identifier(name):
    return "".join(character for character in name.lower() if character.isalnum())


def _first_matching_column(columns, aliases):
    return next((column for column in columns if _normalise_identifier(column) in aliases), None)


def _discover_gold_table(cursor):
    """Select the best business table using metadata, never a fixed table name."""
    cursor.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' "
        "AND table_schema NOT IN ('information_schema', 'pg_catalog')"
    )
    candidates = []
    for schema_name, table_name in cursor.fetchall():
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema_name, table_name),
        )
        columns = [row[0] for row in cursor.fetchall()]
        risk_column = _first_matching_column(columns, RISK_COLUMN_ALIASES)
        channel_column = _first_matching_column(columns, CHANNEL_COLUMN_ALIASES)
        normalised_table = _normalise_identifier(table_name)
        score = (100 if "gold" in normalised_table else 0) + (20 if "transaction" in normalised_table else 0)
        score += 10 if risk_column else 0
        score += 10 if channel_column else 0
        if score:
            candidates.append((score, schema_name, table_name, risk_column, channel_column))
    if not candidates:
        raise RuntimeError("No business table could be discovered from information_schema")
    return max(candidates, key=lambda candidate: candidate[0])


def _set_grouped_metrics(cursor, schema_name, table_name, column_name, label_name, *metrics):
    if not column_name:
        raise RuntimeError(f"No {label_name} column could be mapped in discovered gold table")
    query = sql.SQL("SELECT {}::text, COUNT(*) FROM {}.{} WHERE {} IS NOT NULL GROUP BY 1").format(
        sql.Identifier(column_name),
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        sql.Identifier(column_name),
    )
    cursor.execute(query)
    for value, count in cursor.fetchall():
        for metric in metrics:
            metric.labels(**{label_name: value}).set(count)


def refresh_postgres():
    try:
        with psycopg2.connect(PG_DSN) as connection:
            with connection.cursor() as cursor:
                _, schema_name, table_name, risk_column, channel_column = _discover_gold_table(cursor)
                table = sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {} ").format(table))
                total = cursor.fetchone()[0]
                swam_datamart_total.set(total)
                swam_gold_transactions_total.set(total)
                _set_grouped_metrics(cursor, schema_name, table_name, risk_column, "risk", swam_risk_score_total, swam_gold_risk_score_total)
                _set_grouped_metrics(
                    cursor,
                    schema_name,
                    table_name,
                    channel_column,
                    "channel",
                    swam_payment_channel_total,
                    swam_gold_payment_channel_total,
                )
                log.debug("PostgreSQL gold source discovered as %s.%s", schema_name, table_name)
    except Exception as exc:
        _report_refresh("postgres", exc)
    else:
        _report_refresh("postgres")


def refresh_loop():
    while True:
        refresh_kafka()
        refresh_minio()
        refresh_postgres()
        time.sleep(REFRESH_INTERVAL)


def main():
    log.info("SWAM business metrics exporter starting on :%d (refresh every %ds)", LISTEN_PORT, REFRESH_INTERVAL)
    start_http_server(LISTEN_PORT)
    Thread(target=refresh_loop, daemon=True).start()
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
