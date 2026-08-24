#!/usr/bin/env python3
"""
payments.gold -> payments.gold.flat flattener
================================================
Permanent pipeline component — this is the production path from
payments.gold into PostgreSQL. It replaces the earlier Python-bridge
sink (gold-sink, now paused) with a real Kafka Connect JDBC Sink
connector (gold-transactions-sink) writing to gold_transactions, a
single flat table (see sql/gold_transactions_schema.sql).

Consumes the Flink-enriched "payments.gold" records (nested JSON: a
"transaction" sub-object plus top-level enrichment fields added by
job4_optimize.py / common/iso_standards.py) and republishes a FLAT JSON
record per transaction to "payments.gold.flat".

The flat record's keys match the gold_transactions columns 1:1, so the
Kafka Connect JDBC sink connector
(io.debezium.connector.jdbc.JdbcSinkConnector, insert.mode=upsert,
primary.key.fields=authorization_code) can write it directly without any
further transformation.

Runs as a long-lived Kubernetes Deployment in the kafka-connect namespace
(same pattern gold-sink used), so KAFKA_BOOTSTRAP defaults to the
in-cluster bootstrap service rather than a host port-forward address.
"""
import json
import logging
import os
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [gold-flattener] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "hps-cluster-kafka-bootstrap.kafka.svc:9092")
SOURCE_TOPIC    = os.getenv("SOURCE_TOPIC", "payments.gold")
TARGET_TOPIC    = os.getenv("TARGET_TOPIC", "payments.gold.flat")
GROUP_ID        = os.getenv("GROUP_ID", "gold-flattener")


def extract_row(record: dict) -> dict | None:
    """Flatten a payments.gold record into gold_transactions columns."""
    tx = record.get("transaction", {})
    auth = str(tx.get("AUTHORIZATION_CODE", "")).strip()
    if not auth or auth == "__UNKNOWN__":
        return None

    try:
        amount = float(str(tx.get("TRANSACTION_AMOUNT", 0) or 0).replace(",", "."))
    except (ValueError, TypeError):
        amount = 0.0

    return {
        "authorization_code": auth,
        "message_type":       str(tx.get("MESSAGE_TYPE", "")).strip() or None,
        "transaction_amount": amount,
        "currency_code":      str(tx.get("TRANSACTION_CURRENCY", "")).strip() or None,
        "currency_alpha":     record.get("currency_alpha"),
        "issuing_bank":       str(tx.get("ISSUING_BANK", "")).strip() or None,
        "card_type":          str(tx.get("CARD_TYPE", "")).strip() or None,
        "card_scheme":        record.get("card_scheme"),
        "payment_channel":    record.get("payment_channel"),
        "risk_score":         record.get("risk_score"),
        "mti_name":           record.get("mti_name"),
        "mcc_description":    record.get("mcc_description"),
        "matching_status":    str(tx.get("MATCHING_STATUS", "")).strip() or None,
        "reject_code":        str(tx.get("REJECT_CODE", "")).strip() or None,
        "processed_at":       record.get("processed_at") or record.get("optimized_at"),
        "source_system":      record.get("source_system", "SWAM"),
        "pipeline_version":   record.get("pipeline_version"),
    }


# Kafka Connect schema envelope for the flat record. The Debezium JDBC Sink
# connector's SinkRecordDescriptor.Builder needs a non-null value Schema to
# determine column types — schemaless JSON (the original gold_flattener
# output, and the cause of the original fact-transactions-sink failure)
# caused an NPE in isFlattened(). Wrapping every record in this
# {"schema": ..., "payload": ...} envelope, paired with
# value.converter.schemas.enable=true on the connector, gives the connector
# the schema it needs. Kept deliberately (rather than switching to plain
# schemaless JSON) since this is the proven-working approach in this repo.
_STRING_FIELDS = (
    "authorization_code", "message_type", "currency_code", "currency_alpha",
    "issuing_bank", "card_type", "card_scheme", "payment_channel",
    "risk_score", "mti_name", "mcc_description", "matching_status",
    "reject_code", "processed_at", "source_system", "pipeline_version",
)
_CONNECT_SCHEMA = {
    "type": "struct",
    "name": "gold_transactions",
    "optional": False,
    "fields": (
        [{"field": "authorization_code", "type": "string", "optional": False}]
        + [{"field": f, "type": "string", "optional": True} for f in _STRING_FIELDS if f != "authorization_code"]
        + [{"field": "transaction_amount", "type": "double", "optional": True}]
    ),
}


def envelope(row: dict) -> dict:
    """Wrap a flat row in the Kafka Connect schema+payload envelope."""
    return {"schema": _CONNECT_SCHEMA, "payload": row}


def wait_for_kafka(bootstrap: str, retries: int = 20):
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                SOURCE_TOPIC,
                bootstrap_servers=[bootstrap],
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
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
    log.info("Gold flattener starting — %s -> %s", SOURCE_TOPIC, TARGET_TOPIC)
    consumer, producer = wait_for_kafka(KAFKA_BOOTSTRAP)

    sent = skipped = 0
    for msg in consumer:
        try:
            row = extract_row(msg.value)
        except Exception as exc:
            log.error("Flatten error offset=%d: %s", msg.offset, exc)
            skipped += 1
            continue

        if row is None:
            skipped += 1
            continue

        producer.send(TARGET_TOPIC, key=row["authorization_code"].encode("utf-8"), value=envelope(row))
        sent += 1
        if sent % 10 == 0:
            producer.flush()
            log.info("Flattened %d records (skipped=%d)", sent, skipped)


if __name__ == "__main__":
    main()
