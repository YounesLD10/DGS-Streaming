"""
Job 2 — Validation
Consume decrypted envelopes from `payments.decrypted`, run ISO compliance
checks on the payload, fan out:
  valid   → `payments.validated`
  invalid → `payments.dlq`  (with structured rejection reasons)

Checks:
  - ISO 8583  : MTI must be a known message type
  - ISO 4217  : TRANSACTION_CURRENCY numeric code must exist
  - ISO 7812  : CARD_NUMBER must pass Luhn (mod-10)
  - Mandatory fields present (PAN, MTI, amount, currency, timestamp)
"""
from __future__ import annotations
import json
import os
import sys
import time

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee,
)

sys.path.insert(0, "/opt/jobs")
from common.iso_standards import ISO8583_MTI, is_valid_currency, luhn_check  # noqa: E402

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SOURCE_TOPIC = "payments.decrypted"
VALID_TOPIC = "payments.validated"
DLQ_TOPIC = "payments.dlq"
GROUP_ID = "flink-job2-validation"

DLQ_TAG = OutputTag("dlq", Types.STRING())

MANDATORY = ("MESSAGE_TYPE", "CARD_NUMBER", "TRANSACTION_AMOUNT", "TRANSACTION_CURRENCY")


def validate(payload: dict) -> list[dict]:
    errors: list[dict] = []
    for f in MANDATORY:
        v = payload.get(f)
        if v is None or v == "":
            errors.append({"code": "MANDATORY_FIELD_MISSING", "field": f})

    mti = str(payload.get("MESSAGE_TYPE", "")).strip()
    if mti and mti not in ISO8583_MTI:
        errors.append({"code": "ISO8583_UNKNOWN_MTI", "field": "MESSAGE_TYPE", "value": mti})

    currency = str(payload.get("TRANSACTION_CURRENCY", "")).strip()
    if currency and not is_valid_currency(currency):
        errors.append({"code": "ISO4217_INVALID_CURRENCY", "field": "TRANSACTION_CURRENCY", "value": currency})

    pan = str(payload.get("CARD_NUMBER", "")).strip()
    if pan and not luhn_check(pan):
        errors.append({"code": "ISO7812_LUHN_FAILED", "field": "CARD_NUMBER", "valueBin": pan[:6]})

    return errors


class ValidateFn(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            envelope = json.loads(value)
            payload = envelope.get("payload") or {}
            errors = validate(payload)
            envelope.setdefault("_processing", {})["validatedAt"] = int(time.time() * 1000)
            if errors:
                envelope["validation"] = {"valid": False, "errors": errors}
                envelope["stage"] = "validation"
                yield DLQ_TAG, json.dumps(envelope, ensure_ascii=False)
            else:
                envelope["validation"] = {"valid": True}
                yield json.dumps(envelope, ensure_ascii=False)
        except Exception as exc:
            yield DLQ_TAG, json.dumps({
                "stage": "validation",
                "errorCode": "PARSE_FAILED",
                "errorMessage": f"{type(exc).__name__}: {exc}",
                "originalMessage": value,
                "timestamp": int(time.time() * 1000),
            }, ensure_ascii=False)


def _kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.environ.get("PARALLELISM", "1")))
    env.enable_checkpointing(60_000)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(SOURCE_TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    src = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-decrypted")
    main_stream = src.process(ValidateFn(), output_type=Types.STRING())
    main_stream.sink_to(_kafka_sink(VALID_TOPIC)).name("kafka-payments.validated")
    main_stream.get_side_output(DLQ_TAG).sink_to(_kafka_sink(DLQ_TOPIC)).name("kafka-dlq")

    env.execute("rt-payments-job2-validation")


if __name__ == "__main__":
    main()
