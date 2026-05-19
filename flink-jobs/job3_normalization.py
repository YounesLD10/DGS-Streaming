"""
Job 3 — Normalisation
Consume validated envelopes from `payments.validated` and emit a canonical
record to `payments.normalized`. The canonical schema is loosely modelled on
ISO 20022 pacs.008 concepts (transaction, debtor card, merchant, amount with
currency) so downstream consumers don't need to know about ISO 8583 quirks.

  - ISO 8601  : all dates normalized to "YYYY-MM-DDTHH:MM:SSZ"
  - PCI DSS   : PAN masked (first 6 + last 4)
  - ISO 4217  : currency expanded to {code, alpha, minorUnits}
  - ISO 18245 : MCC enriched with description
  - ISO 8583  : MTI expanded with name
"""
from __future__ import annotations
import json
import os
import sys
import time

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema, DeliveryGuarantee,
)
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy

sys.path.insert(0, "/opt/jobs")
from common.parquet_sink import ParquetWriterFn  # noqa: E402
from common.iso_standards import (  # noqa: E402
    ISO8583_MTI, currency_alpha, currency_minor_units, mcc_description,
    mask_pan, pan_bin, card_scheme, to_iso8601, business_date_from,
)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
S3_BUCKET = os.environ.get("S3_BUCKET", "rt-payments")
SOURCE_TOPIC = "payments.validated"
SINK_TOPIC = "payments.normalized"
GROUP_ID = "flink-job3-normalization"


def _amount(value, currency_code: str) -> dict | None:
    if value in (None, ""):
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return {
        "value": v,
        "currencyCode": currency_code,
        "currencyAlpha": currency_alpha(currency_code),
        "minorUnits": currency_minor_units(currency_code),
    }


def normalize(envelope: dict) -> dict:
    p = envelope.get("payload") or {}
    mti = str(p.get("MESSAGE_TYPE", "")).strip()
    currency = str(p.get("TRANSACTION_CURRENCY", "")).strip()
    billing_currency = str(p.get("BILLING_CURRENCY", "")).strip() or currency
    settlement_currency = str(p.get("ISS_SETTLEMENT_CURRENCY", "")).strip() or currency
    pan = str(p.get("CARD_NUMBER", "")).strip()
    mcc = str(p.get("CARD_ACCEPTOR_ACTIVITY", "")).strip()
    txn_date = p.get("TRANSACTION_LOCAL_DATE") or p.get("TRANSMISSION_DATE_AND_TIME")

    canonical = {
        "msgId": envelope.get("eventId"),
        "schema": "rt-payments-canonical-v1",
        "creDtTm": to_iso8601(p.get("TRANSMISSION_DATE_AND_TIME")),
        "businessDate": business_date_from(p.get("BUSINESS_DATE") or txn_date),
        "iso8583": {
            "mti": mti,
            "mtiName": ISO8583_MTI.get(mti),
            "processingCode": p.get("PROCESSING_CODE"),
            "actionCode": p.get("ACTION_CODE"),
            "responseCode": p.get("ORIGINAL_ACTION_CODE"),
        },
        "transaction": {
            "amount": _amount(p.get("TRANSACTION_AMOUNT"), currency),
            "billingAmount": _amount(p.get("BILLING_AMOUNT"), billing_currency),
            "settlementAmount": _amount(p.get("ISS_SETTLEMENT_AMOUNT"), settlement_currency),
            "cashBackAmount": _amount(p.get("CASH_BACK_AMOUNT"), currency),
            "transactionLocalDate": to_iso8601(p.get("TRANSACTION_LOCAL_DATE")),
            "captureDate": to_iso8601(p.get("CAPTURE_DATE")),
        },
        "card": {
            "panMasked": mask_pan(pan),
            "panBin": pan_bin(pan),
            "scheme": card_scheme(pan),
            "expiryDate": to_iso8601(p.get("END_EXPIRY_DATE")),
            "sequenceNumber": p.get("CARD_SEQUENCE_NUMBER"),
        },
        "merchant": {
            "id": p.get("CARD_ACCEPTOR_ID"),
            "name": (p.get("CARD_ACC_NAME_ADDRESS") or "").strip() or None,
            "mcc": mcc or None,
            "mccDescription": mcc_description(mcc),
            "terminalId": p.get("CARD_ACCEPTOR_TERM_ID"),
        },
        "acquirer": {
            "institutionCode": p.get("ACQUIRER_INSTITUTION_CODE"),
            "bank": p.get("ACQUIRER_BANK"),
            "countryCode": p.get("ACQUIRING_COUNTRY_CODE"),
        },
        "issuer": {
            "bank": p.get("ISSUING_BANK"),
            "networkCode": p.get("NETWORK_CODE"),
        },
        "audit": {
            "stan": p.get("INTERNAL_STAN"),
            "rrn": p.get("REFERENCE_NUMBER"),
            "authCode": p.get("AUTHORIZATION_CODE"),
            "authSource": p.get("AUTHORIZATION_SOURCE"),
            "originalAuthCode": p.get("ORIGINAL_AUTHORIZATION_CODE"),
        },
        "_processing": {
            **(envelope.get("_processing") or {}),
            "normalizedAt": int(time.time() * 1000),
        },
    }
    return canonical


class NormalizeFn(MapFunction):
    def map(self, value):
        envelope = json.loads(value)
        return json.dumps(normalize(envelope), ensure_ascii=False)


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

    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(SINK_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    s3_sink = (
        FileSink.for_row_format(f"s3a://{S3_BUCKET}/normalized", Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(
            OutputFileConfig.builder().with_part_prefix("normalized").with_part_suffix(".jsonl").build()
        )
        .with_rolling_policy(RollingPolicy.default_rolling_policy(
            part_size=64 * 1024 * 1024,
            rollover_interval=60_000,
            inactivity_interval=30_000,
        ))
        .build()
    )

    normalized = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-validated")
        .map(NormalizeFn(), output_type=Types.STRING())
    )
    normalized.sink_to(sink).name("kafka-payments.normalized")
    normalized.sink_to(s3_sink).name("minio-normalized-jsonl")
    normalized.map(ParquetWriterFn(S3_BUCKET, "normalized-parquet"), output_type=Types.STRING()).print()

    env.execute("rt-payments-job3-normalization")


if __name__ == "__main__":
    main()
