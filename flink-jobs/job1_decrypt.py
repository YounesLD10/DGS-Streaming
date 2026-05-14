"""
Job 1 — Decryption
==================
Source  : Kafka 'payments'               (Fernet-encrypted JSON envelopes)
Process : DecryptFn (ProcessFunction)
            • Parse outer JSON envelope
            • Decrypt Fernet-encoded payload field
            • Attach _meta.decryptedAt timestamp
            • On any error → OutputTag → payments.dlq
Sink    : MinIO  rt-payments/bronze/     (BronzeMinioFn MapFunction, pass-through)
          Kafka  payments.decrypted      (main output)
          Kafka  payments.dlq            (side output — decryption errors)

Architectural notes
-------------------
* DecryptFn initialises the Fernet cipher in open() so the non-picklable
  Fernet object is never serialised by the Flink task scheduler.
* MinIO writes happen inside a MapFunction that is transparent to the
  downstream Kafka sink — the value passes through unchanged.
* DLQ records never carry the raw encrypted payload to avoid leaking
  encrypted data into the dead-letter topic.
* Checkpointing is set to EXACTLY_ONCE so Flink state is consistent
  across restarts; the Kafka sink uses AT_LEAST_ONCE (simpler, no
  transactional producer configuration required in the PoC).
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

# Make the common package importable whether running locally or inside the
# container (where PYTHONPATH=/opt/jobs takes care of it automatically).
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
from pyflink.datastream.functions import MapFunction, ProcessFunction

from common.config import (
    CHECKPOINT_INTERVAL_MS,
    FERNET_KEY,
    KAFKA_BOOTSTRAP,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    TOPIC_DECRYPTED,
    TOPIC_DLQ,
    TOPIC_PAYMENTS,
)
from common.crypto import fernet_decrypt, make_fernet
from common.minio_sink import ensure_bucket, get_minio_client, write_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job1-decrypt] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# Side-output tag for records that fail decryption
DLQ_TAG = OutputTag("dlq-decrypt", Types.STRING())

BRONZE_PREFIX = "bronze"


# ── ProcessFunction ────────────────────────────────────────────────────────────

class DecryptFn(ProcessFunction):
    """Decrypt the Fernet payload in each Kafka envelope.

    Main output : JSON string with transaction fields + _meta block
    Side output : DLQ_TAG — error envelope (no encrypted data included)
    """

    def __init__(self, fernet_key: str) -> None:
        self._key = fernet_key
        self._fernet = None  # initialised in open() to avoid pickle issues

    def open(self, runtime_context) -> None:
        self._fernet = make_fernet(self._key)
        log.info("DecryptFn ready (Fernet cipher initialised)")

    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            envelope = json.loads(value)
            raw_payload = envelope.get("payload", "")

            if not raw_payload:
                raise ValueError("envelope.payload is empty")

            decrypted_str = fernet_decrypt(self._fernet, raw_payload)
            transaction = json.loads(decrypted_str)

            record = {
                "eventId":     envelope.get("eventId"),
                "table":       envelope.get("table"),
                "operation":   envelope.get("operation"),
                "kafka_ts":    envelope.get("timestamp"),
                "transaction": transaction,
                "_meta": {
                    "decryptedAt": datetime.now(timezone.utc).isoformat(),
                    "job":         "job1-decrypt",
                },
            }
            yield json.dumps(record, ensure_ascii=False)

        except Exception as exc:
            # Build a DLQ record that carries only metadata — no raw payload
            try:
                env_safe = json.loads(value)
                env_safe.pop("payload", None)
            except Exception:
                env_safe = {}

            dlq = {
                "eventId":      env_safe.get("eventId"),
                "error":        str(exc),
                "error_type":   type(exc).__name__,
                "envelope_meta": {
                    k: v for k, v in env_safe.items() if k != "payload"
                },
                "_meta": {
                    "errorAt": datetime.now(timezone.utc).isoformat(),
                    "job":     "job1-decrypt",
                },
            }
            log.warning("Decrypt error eventId=%s: %s", env_safe.get("eventId"), exc)
            ctx.output(DLQ_TAG, json.dumps(dlq, ensure_ascii=False))


# ── MapFunction (MinIO bronze write, pass-through) ─────────────────────────────

class BronzeMinioFn(MapFunction):
    """Write each decrypted record to MinIO bronze layer.

    The value is returned unchanged so the downstream Kafka sink
    receives the same record.  MinIO failures are logged but do NOT
    fail the stream — a missing bronze object is preferable to
    dropping records from the main pipeline in a PoC context.
    """

    def open(self, runtime_context) -> None:
        self._client = get_minio_client(
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        )
        ensure_bucket(self._client, MINIO_BUCKET)
        log.info("BronzeMinioFn: MinIO client ready (bucket=%s)", MINIO_BUCKET)

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            key = write_record(self._client, MINIO_BUCKET, BRONZE_PREFIX, record)
            log.debug("bronze → %s/%s", MINIO_BUCKET, key)
        except Exception as exc:
            log.error("MinIO bronze write failed: %s", exc)
        return value


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
    if not FERNET_KEY:
        log.error("FERNET_KEY env var is required — aborting")
        sys.exit(1)

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
        .set_topics(TOPIC_PAYMENTS)
        .set_group_id("flink-job1-decrypt")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "payments-source",
    )

    # Decrypt: main output = decrypted records, side output = DLQ errors
    processed = (
        raw
        .process(DecryptFn(FERNET_KEY), output_type=Types.STRING())
        .uid("decrypt-fn")
        .name("DecryptFn")
    )

    dlq_stream = processed.get_side_output(DLQ_TAG)

    # Write valid records to MinIO bronze, then forward to Kafka
    (
        processed
        .map(BronzeMinioFn(), output_type=Types.STRING())
        .uid("bronze-minio-fn")
        .name("BronzeMinioFn")
        .sink_to(_kafka_sink(TOPIC_DECRYPTED))
        .uid("payments-decrypted-sink")
        .name("payments.decrypted sink")
    )

    # Route DLQ errors to payments.dlq
    (
        dlq_stream
        .sink_to(_kafka_sink(TOPIC_DLQ))
        .uid("dlq-decrypt-sink")
        .name("payments.dlq sink (decrypt errors)")
    )

    log.info(
        "Submitting job: rt-payments-job1-decryption | "
        "source=%s | out=%s | dlq=%s",
        TOPIC_PAYMENTS, TOPIC_DECRYPTED, TOPIC_DLQ,
    )
    env.execute("rt-payments-job1-decryption")


if __name__ == "__main__":
    main()
