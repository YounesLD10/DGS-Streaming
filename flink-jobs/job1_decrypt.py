"""
Job 1 — Décryption
  Source : Kafka topic 'payments'
  Process: Decrypt Fernet payload → parse JSON transaction fields
           Add metadata (decrypted_at, _job)
  Sink   : MinIO Bronze  (rt-payments/bronze/)
           Kafka topic   'payments.decrypted'
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    BATCH_SIZE, flush_to_minio,
    get_consumer, get_minio, get_producer, now_iso,
)
from cryptography.fernet import Fernet, InvalidToken

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job1-decrypt] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

INPUT_TOPIC  = "payments"
OUTPUT_TOPIC = "payments.decrypted"
BRONZE       = "bronze"


def run(fernet_key: str) -> None:
    if not fernet_key:
        log.error("FERNET_KEY is required. Pass via --key or FERNET_KEY env var.")
        sys.exit(1)

    fernet   = Fernet(fernet_key.encode() if isinstance(fernet_key, str) else fernet_key)
    minio    = get_minio()
    producer = get_producer()
    consumer = get_consumer(INPUT_TOPIC, "job1-decrypt")

    log.info(f"Consuming '{INPUT_TOPIC}' → decrypt → Bronze + '{OUTPUT_TOPIC}'")

    batch      = []
    processed  = 0
    errors     = 0

    try:
        for msg in consumer:
            envelope = msg.value
            try:
                raw_payload = envelope.get("payload", "")
                decrypted   = fernet.decrypt(raw_payload.encode()).decode("utf-8")
                transaction = json.loads(decrypted)

                record = {
                    "eventId":      envelope.get("eventId"),
                    "table":        envelope.get("table"),
                    "operation":    envelope.get("operation"),
                    "kafka_ts":     envelope.get("timestamp"),
                    "transaction":  transaction,
                    "decrypted_at": now_iso(),
                    "_job":         "decrypt",
                }

                batch.append(record)
                producer.send(OUTPUT_TOPIC, value=record)
                processed += 1

                if len(batch) >= BATCH_SIZE:
                    flush_to_minio(minio, batch, BRONZE, log)
                    log.info(f"Processed: {processed} | errors: {errors}")
                    batch = []

            except InvalidToken:
                errors += 1
                log.warning(f"[SKIP] Invalid Fernet token — eventId={envelope.get('eventId')}")
            except Exception as exc:
                errors += 1
                log.error(f"[ERROR] {exc} — eventId={envelope.get('eventId')}")

    except Exception as exc:
        log.error(f"Consumer error: {exc}")
    finally:
        if batch:
            flush_to_minio(minio, batch, BRONZE, log)
        producer.flush()
        log.info(f"Done. processed={processed} errors={errors}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--key", default=os.getenv("FERNET_KEY", ""), help="Fernet encryption key")
    args = p.parse_args()
    run(args.key)
