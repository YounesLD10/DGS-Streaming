"""
Job 2 — Validation
  Source : Kafka topic 'payments.decrypted'
  Process: Validate mandatory fields and business rules
           VALID   → forward to 'payments.validated'
           INVALID → send to  'payments.dlq' with reject_reason
  Sink   : Kafka 'payments.validated' (valid)
           Kafka 'payments.dlq'       (invalid)
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from common import KAFKA_BOOTSTRAP, get_consumer, get_producer, now_iso

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job2-validate] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

INPUT_TOPIC   = "payments.decrypted"
VALID_TOPIC   = "payments.validated"
INVALID_TOPIC = "payments.dlq"

MANDATORY_FIELDS = [
    "MESSAGE_TYPE",
    "TRANSACTION_AMOUNT",
    "TRANSACTION_CURRENCY",
    "ISSUING_BANK",
    "CARD_TYPE",
]


def _validate(tx: dict) -> tuple[bool, str]:
    """Return (is_valid, reject_reason)."""
    for field in MANDATORY_FIELDS:
        if not tx.get(field) or str(tx[field]).strip() == "":
            return False, f"MISSING_FIELD:{field}"

    msg_type = str(tx.get("MESSAGE_TYPE", "")).strip()
    if not msg_type.isdigit() or len(msg_type) != 4:
        return False, "INVALID_MESSAGE_TYPE"

    try:
        amount = float(str(tx.get("TRANSACTION_AMOUNT", "0")).replace(",", "."))
    except ValueError:
        return False, "INVALID_AMOUNT_FORMAT"

    if amount <= 0:
        return False, "AMOUNT_NOT_POSITIVE"

    reject_code = str(tx.get("REJECT_CODE", "")).strip()
    if reject_code:
        return False, "REJECTED_BY_BANK"

    return True, ""


def run() -> None:
    producer = get_producer()
    consumer = get_consumer(INPUT_TOPIC, "job2-validate")

    log.info(f"Consuming '{INPUT_TOPIC}' → validate → '{VALID_TOPIC}' / '{INVALID_TOPIC}'")

    valid_count   = 0
    invalid_count = 0

    try:
        for msg in consumer:
            record = msg.value
            tx     = record.get("transaction", {})

            is_valid, reason = _validate(tx)

            record["validation_status"] = "VALID" if is_valid else "INVALID"
            record["validated_at"]      = now_iso()
            record["_job"]              = "validate"

            if is_valid:
                producer.send(VALID_TOPIC, value=record)
                valid_count += 1
            else:
                record["reject_reason"] = reason
                producer.send(INVALID_TOPIC, value=record)
                invalid_count += 1

            total = valid_count + invalid_count
            if total % BATCH_SIZE == 0:
                log.info(f"Processed {total} | valid={valid_count} invalid={invalid_count}")

    except Exception as exc:
        log.error(f"Consumer error: {exc}")
    finally:
        producer.flush()
        log.info(f"Done. valid={valid_count} invalid={invalid_count}")


BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

if __name__ == "__main__":
    run()
