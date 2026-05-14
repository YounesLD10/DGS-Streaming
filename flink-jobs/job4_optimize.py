"""
Job 4 — Optimisation
====================
Source  : Kafka 'payments.normalized'
Process : KeyedStream by AUTHORIZATION_CODE
          OptimizeFn (KeyedProcessFunction)
            ① Deduplication via Flink ValueState
               – AUTHORIZATION_CODE seen before → record dropped
               – New code                        → processed, state marked seen
            ② Payment channel routing
               PRODUCT_CODE = "6"  →  payment_channel = "SO_CARTE"
               anything else       →  payment_channel = "SO_MOBILE"
            ③ Risk scoring
               HIGH   : REJECT_CODE was filled  OR  TRANSACTION_AMOUNT > 10_000
               MEDIUM : MATCHING_STATUS not in {U, I, L}  OR  AMOUNT == 0
               LOW    : all checks pass
            ④ Add fields: payment_channel, risk_score, optimized_at (UTC ISO 8601)
Sink    : MinIO  rt-payments/gold/   (GoldMinioFn MapFunction, pass-through)

Architectural notes
-------------------
* KeyedProcessFunction gives each AUTHORIZATION_CODE its own isolated
  ValueState partition — deduplication is exact and survives restarts
  because Flink snapshots the state during checkpointing.
* AuthCodeKeySelector is a class (not a lambda) to guarantee clean
  serialization by the Flink task scheduler across JVM/Python boundary.
* A missing or empty AUTHORIZATION_CODE is keyed as "__UNKNOWN__" so it
  is never skipped by the deduplication logic (empty strings are not a
  valid business key and would incorrectly deduplicate all such records).
* Risk scoring evaluates HIGH first so a record with both a REJECT_CODE
  and an abnormal MATCHING_STATUS is always bucketed HIGH.
* Gold objects in MinIO carry fully enriched, deduplicated records —
  exactly what a downstream analytical query or dashboard should read.
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
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

from common.config import (
    CHECKPOINT_INTERVAL_MS,
    KAFKA_BOOTSTRAP,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    TOPIC_NORMALIZED,
)
from common.minio_sink import ensure_bucket, get_minio_client, write_record

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job4-optimize] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

GOLD_PREFIX = "gold"
_VALID_MATCHING_STATUSES = {"U", "I", "L"}
_UNKNOWN_KEY = "__UNKNOWN__"


# ── KeySelector ────────────────────────────────────────────────────────────────

class AuthCodeKeySelector:
    """Extract AUTHORIZATION_CODE from a JSON-encoded normalised record.

    Returns __UNKNOWN__ when the code is absent or empty so those records
    are grouped together but never silently deduplicated against each other.
    """

    def get_key(self, value: str) -> str:
        try:
            record = json.loads(value)
            tx = record.get("transaction", record)
            code = str(tx.get("AUTHORIZATION_CODE", "")).strip()
            return code if code else _UNKNOWN_KEY
        except Exception:
            return _UNKNOWN_KEY


# ── Business logic helpers ─────────────────────────────────────────────────────

def _payment_channel(tx: dict) -> str:
    return "SO_CARTE" if str(tx.get("PRODUCT_CODE", "")).strip() == "6" else "SO_MOBILE"


def _risk_score(tx: dict) -> str:
    """Compute a three-tier risk score.

    Evaluated in priority order: HIGH → MEDIUM → LOW.
    """
    reject_code = str(tx.get("REJECT_CODE", "")).strip()
    matching    = str(tx.get("MATCHING_STATUS", "")).strip()

    try:
        amount = float(
            str(tx.get("TRANSACTION_AMOUNT", 0)).replace(",", ".")
        )
    except (ValueError, TypeError):
        amount = 0.0

    if reject_code or amount > 10_000:
        return "HIGH"
    if matching not in _VALID_MATCHING_STATUSES or amount == 0:
        return "MEDIUM"
    return "LOW"


# ── KeyedProcessFunction ───────────────────────────────────────────────────────

class OptimizeFn(KeyedProcessFunction):
    """Deduplicate by AUTHORIZATION_CODE and enrich with channel + risk fields.

    State layout
    ------------
    _seen (ValueState[bool])
        Persisted per keyed partition in the Flink state backend.
        True  → this AUTHORIZATION_CODE was already processed; drop.
        None  → first occurrence; process and set True.
    """

    def open(self, runtime_context) -> None:
        self._seen = runtime_context.get_state(
            ValueStateDescriptor("seen", Types.BOOLEAN())
        )
        log.info("OptimizeFn ready (ValueState initialised)")

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        current_key = ctx.get_current_key()

        # ── Deduplication ──────────────────────────────────────────────────────
        # Records keyed __UNKNOWN__ are not deduplicated against each other
        # because they lack a real business key.
        if current_key != _UNKNOWN_KEY and self._seen.value():
            log.debug("Duplicate AUTHORIZATION_CODE=%s — skipped", current_key)
            return  # drop duplicate

        if current_key != _UNKNOWN_KEY:
            self._seen.update(True)

        # ── Enrichment ────────────────────────────────────────────────────────
        try:
            record = json.loads(value)
            tx = record.get("transaction", {})

            record["payment_channel"] = _payment_channel(tx)
            record["risk_score"]      = _risk_score(tx)
            record["optimized_at"]    = datetime.now(timezone.utc).isoformat()
            record.setdefault("_meta", {})["job"] = "job4-optimize"

            log.debug(
                "Processed AUTHORIZATION_CODE=%s channel=%s risk=%s",
                current_key,
                record["payment_channel"],
                record["risk_score"],
            )
            yield json.dumps(record, ensure_ascii=False)

        except Exception as exc:
            log.error(
                "Enrichment error AUTHORIZATION_CODE=%s: %s", current_key, exc
            )
            raise


# ── MapFunction (MinIO gold write, pass-through) ───────────────────────────────

class GoldMinioFn(MapFunction):
    """Write each optimised record to MinIO gold layer.

    Gold records are fully enriched, deduplicated, and ready for
    analytical consumption — they represent the final state of the pipeline.
    """

    def open(self, runtime_context) -> None:
        self._client = get_minio_client(
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        )
        ensure_bucket(self._client, MINIO_BUCKET)
        log.info("GoldMinioFn: MinIO client ready (bucket=%s)", MINIO_BUCKET)

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            key = write_record(self._client, MINIO_BUCKET, GOLD_PREFIX, record)
            log.debug("gold → %s/%s", MINIO_BUCKET, key)
        except Exception as exc:
            log.error("MinIO gold write failed: %s", exc)
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
        .set_topics(TOPIC_NORMALIZED)
        .set_group_id("flink-job4-optimize")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "payments-normalized-source",
    )

    # Key by AUTHORIZATION_CODE for stateful deduplication
    keyed = raw.key_by(
        AuthCodeKeySelector(),
        key_type=Types.STRING(),
    )

    processed = (
        keyed
        .process(OptimizeFn(), output_type=Types.STRING())
        .uid("optimize-fn")
        .name("OptimizeFn")
    )

    # Write gold records to MinIO; no further Kafka forwarding — gold is terminal
    (
        processed
        .map(GoldMinioFn(), output_type=Types.STRING())
        .uid("gold-minio-fn")
        .name("GoldMinioFn")
    )

    log.info(
        "Submitting job: rt-payments-job4-optimization | source=%s | sink=MinIO gold",
        TOPIC_NORMALIZED,
    )
    env.execute("rt-payments-job4-optimization")


if __name__ == "__main__":
    main()
