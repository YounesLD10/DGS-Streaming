"""
Job 3 — Normalisation
  Source : Kafka topic 'payments.validated'
  Process:
    - Convert date fields DD/MM/YYYY HH:MM → ISO 8601
    - Convert scientific notation amounts (0,11E+02) → float
    - Add metadata: processed_at, source_system, pipeline_version
  Sink   : MinIO Silver (rt-payments/silver/)
           Kafka topic  'payments.normalized'
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import (
    AMOUNT_FIELDS, BATCH_SIZE, DATE_FIELDS,
    flush_to_minio, get_consumer, get_minio,
    get_producer, now_iso, parse_amount, parse_date,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job3-normalize] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

INPUT_TOPIC  = "payments.validated"
OUTPUT_TOPIC = "payments.normalized"
SILVER       = "silver"

SOURCE_SYSTEM      = "HPS_SWAM"
PIPELINE_VERSION   = "1.0.0"


def _normalize(tx: dict) -> dict:
    """Return a new dict with normalized date and amount fields."""
    normalized = dict(tx)

    for field in DATE_FIELDS:
        if field in normalized:
            normalized[field] = parse_date(normalized[field])

    for field in AMOUNT_FIELDS:
        if field in normalized:
            normalized[field] = parse_amount(normalized[field])

    # TRANSACTION_AMOUNT as float (already in list, but ensure it's done)
    if "TRANSACTION_AMOUNT" in normalized:
        val = parse_amount(normalized["TRANSACTION_AMOUNT"])
        normalized["TRANSACTION_AMOUNT"] = val if val is not None else normalized["TRANSACTION_AMOUNT"]

    return normalized


def run() -> None:
    minio    = get_minio()
    producer = get_producer()
    consumer = get_consumer(INPUT_TOPIC, "job3-normalize")

    log.info(f"Consuming '{INPUT_TOPIC}' → normalize → Silver + '{OUTPUT_TOPIC}'")

    batch     = []
    processed = 0

    try:
        for msg in consumer:
            record = msg.value
            tx     = record.get("transaction", {})

            record["transaction"]      = _normalize(tx)
            record["processed_at"]     = now_iso()
            record["source_system"]    = SOURCE_SYSTEM
            record["pipeline_version"] = PIPELINE_VERSION
            record["_job"]             = "normalize"

            batch.append(record)
            producer.send(OUTPUT_TOPIC, value=record)
            processed += 1

            if len(batch) >= BATCH_SIZE:
                flush_to_minio(minio, batch, SILVER, log)
                log.info(f"Processed: {processed}")
                batch = []

    except Exception as exc:
        log.error(f"Consumer error: {exc}")
    finally:
        if batch:
            flush_to_minio(minio, batch, SILVER, log)
        producer.flush()
        log.info(f"Done. processed={processed}")


if __name__ == "__main__":
    run()
