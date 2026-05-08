"""
Job 4 — Optimisation
  Source : Kafka topic 'payments.normalized'
  Process:
    - Deduplication by AUTHORIZATION_CODE (in-memory set, PoC)
    - Payment channel routing:
        PRODUCT_CODE = 6  → payment_channel = SO_CARTE
        PRODUCT_CODE ≠ 6  → payment_channel = SO_MOBILE
    - Risk scoring:
        HIGH   → REJECT_CODE filled OR TRANSACTION_AMOUNT > 10000
        MEDIUM → MATCHING_STATUS not in {U, I, L} OR AMOUNT = 0
        LOW    → all normal
  Sink   : MinIO Gold (rt-payments/gold/)
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import BATCH_SIZE, flush_to_minio, get_consumer, get_minio, now_iso

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job4-optimize] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

INPUT_TOPIC     = "payments.normalized"
GOLD            = "gold"
VALID_STATUSES  = {"U", "I", "L"}


def _channel(tx: dict) -> str:
    return "SO_CARTE" if str(tx.get("PRODUCT_CODE", "")).strip() == "6" else "SO_MOBILE"


def _risk(tx: dict) -> str:
    reject_code     = str(tx.get("REJECT_CODE", "")).strip()
    matching_status = str(tx.get("MATCHING_STATUS", "")).strip()
    try:
        amount = float(str(tx.get("TRANSACTION_AMOUNT", 0)).replace(",", "."))
    except (ValueError, TypeError):
        amount = 0.0

    if reject_code or amount > 10000:
        return "HIGH"
    if matching_status not in VALID_STATUSES or amount == 0:
        return "MEDIUM"
    return "LOW"


def run() -> None:
    minio    = get_minio()
    consumer = get_consumer(INPUT_TOPIC, "job4-optimize")

    log.info(f"Consuming '{INPUT_TOPIC}' → optimize → Gold")

    seen_auth_codes: set = set()
    batch        = []
    processed    = 0
    duplicates   = 0

    try:
        for msg in consumer:
            record = msg.value
            tx     = record.get("transaction", {})

            auth_code = str(tx.get("AUTHORIZATION_CODE", "")).strip()
            if auth_code and auth_code in seen_auth_codes:
                duplicates += 1
                log.debug(f"[DUP] AUTHORIZATION_CODE={auth_code}")
                continue

            if auth_code:
                seen_auth_codes.add(auth_code)

            record["payment_channel"] = _channel(tx)
            record["risk_score"]      = _risk(tx)
            record["optimized_at"]    = now_iso()
            record["_job"]            = "optimize"

            batch.append(record)
            processed += 1

            if len(batch) >= BATCH_SIZE:
                flush_to_minio(minio, batch, GOLD, log)
                log.info(f"Processed: {processed} | duplicates_skipped: {duplicates}")
                batch = []

    except Exception as exc:
        log.error(f"Consumer error: {exc}")
    finally:
        if batch:
            flush_to_minio(minio, batch, GOLD, log)
        log.info(f"Done. processed={processed} duplicates_skipped={duplicates}")


if __name__ == "__main__":
    run()
