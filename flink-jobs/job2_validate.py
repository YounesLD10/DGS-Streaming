"""
Job 2 — Validation
==================
Source  : Kafka 'payments.decrypted'
Process : ValidateFn (ProcessFunction)
            Validates every rule below in order:
            ① MESSAGE_TYPE    — not null, exactly 4 decimal digits
            ② TRANSACTION_AMOUNT — not null, numeric, > 0
            ③ TRANSACTION_CURRENCY — not null / non-empty
            ④ ISSUING_BANK    — not null / non-empty
            ⑤ CARD_TYPE       — not null / non-empty
            ⑥ REJECT_CODE     — must be empty or absent

            VALID   → add validation_status="VALID"
                    → forward to payments.validated  (main output)
            INVALID → add validation_status="INVALID" + reject_reason
                    → send to payments.dlq            (DLQ_TAG side output)

Sink    : Kafka payments.validated  (main output)
          Kafka payments.dlq        (invalid records via OutputTag)

Architectural notes
-------------------
* All six rules are evaluated in one pass; the first failing rule is
  returned as the reject_reason so operators downstream can filter
  by failure category without re-parsing the record.
* The DLQ record includes the full original record plus metadata so
  an analyst can replay or debug it without touching the main topics.
* No MinIO write in Job 2 — decrypted data is already in bronze
  from Job 1; silver is written by the normalisation job.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import (
    CheckpointingMode,
    OutputTag,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessFunction

from common.config import (
    CHECKPOINT_INTERVAL_MS,
    KAFKA_BOOTSTRAP,
    TOPIC_DECRYPTED,
    TOPIC_DLQ,
    TOPIC_VALIDATED,
)
from common.iso_standards import is_valid_currency, is_valid_mti, luhn_check

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job2-validate] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

DLQ_TAG = OutputTag("dlq-validate", Types.STRING())


# ── Validation logic ───────────────────────────────────────────────────────────

def _non_empty(value) -> bool:
    return value is not None and str(value).strip() != ""


def _validate_transaction(tx: dict) -> tuple[bool, str]:
    """Return (is_valid, reject_reason).

    Applies 9 rules in order (rules ①–⑥ structural, ⑦–⑨ ISO standards).
    Reject reason uses the format RULE_CODE so downstream consumers
    can categorise failures without string parsing.
    """
    # ① MESSAGE_TYPE: not null, exactly 4 decimal digits
    msg_type = str(tx.get("MESSAGE_TYPE", "")).strip()
    if not msg_type:
        return False, "MISSING_MESSAGE_TYPE"
    if not msg_type.isdigit() or len(msg_type) != 4:
        return False, "INVALID_MESSAGE_TYPE_FORMAT"

    # ② TRANSACTION_AMOUNT: not null, numeric, > 0
    raw_amount = tx.get("TRANSACTION_AMOUNT")
    if not _non_empty(raw_amount):
        return False, "MISSING_TRANSACTION_AMOUNT"
    try:
        amount = float(str(raw_amount).replace(",", "."))
    except (ValueError, TypeError):
        return False, "INVALID_TRANSACTION_AMOUNT_FORMAT"
    if amount <= 0:
        return False, "TRANSACTION_AMOUNT_NOT_POSITIVE"

    # ③ TRANSACTION_CURRENCY
    if not _non_empty(tx.get("TRANSACTION_CURRENCY")):
        return False, "MISSING_TRANSACTION_CURRENCY"

    # ④ ISSUING_BANK
    if not _non_empty(tx.get("ISSUING_BANK")):
        return False, "MISSING_ISSUING_BANK"

    # ⑤ CARD_TYPE
    if not _non_empty(tx.get("CARD_TYPE")):
        return False, "MISSING_CARD_TYPE"

    # ⑥ REJECT_CODE must be empty / absent
    if str(tx.get("REJECT_CODE", "")).strip():
        return False, "REJECTED_BY_BANK"

    # ⑦ ISO 8583 — MTI must be a known message type
    if msg_type and not is_valid_mti(msg_type):
        return False, "ISO8583_UNKNOWN_MTI"

    # ⑧ ISO 4217 — Currency must be in the dictionary
    currency = str(tx.get("TRANSACTION_CURRENCY", "")).strip()
    if currency and not is_valid_currency(currency):
        return False, "ISO4217_INVALID_CURRENCY"

    # ⑨ ISO 7812 — Luhn check on CARD_NUMBER
    pan = str(tx.get("CARD_NUMBER", "")).replace("*", "").strip()
    real_digits = [c for c in pan if c.isdigit()]
    if len(real_digits) >= 12 and not luhn_check("".join(real_digits)):
        return False, "ISO7812_LUHN_FAILED"

    return True, ""


# ── ProcessFunction ────────────────────────────────────────────────────────────

class ValidateFn(ProcessFunction):
    """Validate decrypted payment records and route to valid/DLQ streams."""

    def open(self, runtime_context) -> None:
        log.info("ValidateFn ready")

    def process_element(self, value: str, _):
        try:
            record = json.loads(value)
            tx = record.get("transaction", {})

            is_valid, reason = _validate_transaction(tx)
            now = datetime.now(timezone.utc).isoformat()

            record["validation_status"] = "VALID" if is_valid else "INVALID"
            record["validated_at"] = now
            record.setdefault("_meta", {})["job"] = "job2-validate"

            if is_valid:
                log.debug("VALID eventId=%s", record.get("eventId"))
                yield json.dumps(record, ensure_ascii=False)
            else:
                record["reject_reason"] = reason
                log.info(
                    "INVALID eventId=%s reason=%s",
                    record.get("eventId"), reason,
                )
                yield DLQ_TAG, json.dumps(record, ensure_ascii=False)

        except Exception as exc:
            # Malformed records go to DLQ with an PARSE_ERROR reason
            dlq = {
                "raw":          value,
                "reject_reason": "PARSE_ERROR",
                "error":        str(exc),
                "validation_status": "INVALID",
                "_meta": {
                    "errorAt": datetime.now(timezone.utc).isoformat(),
                    "job":     "job2-validate",
                },
            }
            log.error("Parse error in validation: %s", exc)
            yield DLQ_TAG, json.dumps(dlq, ensure_ascii=False)


# ── KafkaSink factory ──────────────────────────────────────────────────────────

def _kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


# ── Job entry point ────────────────────────────────────────────────────────────

def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5_000)
    env.get_checkpoint_config().set_checkpoint_timeout(60_000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC_DECRYPTED)
        .set_group_id("flink-job2-validate")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "payments-decrypted-source",
    )

    processed = (
        raw
        .process(ValidateFn(), output_type=Types.STRING())
        .uid("validate-fn")
        .name("ValidateFn")
    )

    dlq_stream = processed.get_side_output(DLQ_TAG)

    (
        processed
        .sink_to(_kafka_sink(TOPIC_VALIDATED))
        .uid("payments-validated-sink")
        .name("payments.validated sink")
    )

    (
        dlq_stream
        .sink_to(_kafka_sink(TOPIC_DLQ))
        .uid("dlq-validate-sink")
        .name("payments.dlq sink (validation errors)")
    )

    log.info(
        "Submitting job: rt-payments-job2-validation | "
        "source=%s | valid=%s | dlq=%s",
        TOPIC_DECRYPTED, TOPIC_VALIDATED, TOPIC_DLQ,
    )
    env.execute("rt-payments-job2-validation")


if __name__ == "__main__":
    main()
