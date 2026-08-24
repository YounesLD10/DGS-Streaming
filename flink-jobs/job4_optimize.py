"""
Job 4 - Optimisation
====================
Source  : Kafka 'payments.normalized'
Process : KeyedStream by AUTHORIZATION_CODE
          OptimizeFn (KeyedProcessFunction)
            1. Deduplication via Flink ValueState
               - AUTHORIZATION_CODE seen before -> record dropped
               - New code                       -> processed, state marked seen
            2. Payment channel routing
               PRODUCT_CODE = "6" -> payment_channel = "SO_CARTE"
               anything else      -> payment_channel = "SO_MOBILE"
            3. Risk scoring
               HIGH   : TRANSACTION_AMOUNT > 10_000
               MEDIUM : MATCHING_STATUS not in {U, I, L}
               LOW    : all checks pass
               (REJECT_CODE and AMOUNT==0 branches removed — dead code filtered by Job2)
            4. Add fields: payment_channel, risk_score, optimized_at (UTC ISO 8601)
Sink    : MinIO  rt-payments/gold/   (GoldMinioFn MapFunction + .print() terminal)

Architectural notes
-------------------
* KeyedProcessFunction gives each AUTHORIZATION_CODE its own isolated
  ValueState partition - deduplication is exact and survives restarts
  because Flink snapshots the state during checkpointing.

* _extract_auth_code is a plain module-level function so PyFlink's key_by
  can call it directly via callable() - a class with only get_key() would
  not pass the callable() check and would fail at submission time.

* A missing or empty AUTHORIZATION_CODE is keyed as "__UNKNOWN__" so those
  records are never silently deduplicated against each other (empty strings
  are not a valid business key).

* Risk scoring evaluates HIGH first so a record with both a REJECT_CODE
  and an abnormal MATCHING_STATUS is always bucketed HIGH.

* In PyFlink 1.19, SinkFunction is JavaFunctionWrapper — its __init__
  requires a Java object (not Python inheritance). There is no Python-
  subclassable sink base class in the DataStream Python API. The correct
  pattern is MapFunction for the MinIO write (as a side-effect pass-through)
  followed by .print() which wraps PrintSinkFunction (a real Java sink) and
  acts as the terminal operator that drives Flink's execution graph.

* Gold objects in MinIO carry fully enriched, deduplicated records -
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
    KafkaOffsetsInitializer,
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
    TOPIC_GOLD,
    TOPIC_NORMALIZED,
)
from common.minio_sink import ensure_bucket, get_minio_client, write_record
from common.kafka_utils import kafka_sink as _kafka_sink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job4-optimize] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

GOLD_PREFIX = "gold"
_VALID_MATCHING_STATUSES = {"U", "I", "L"}
_UNKNOWN_KEY = "__UNKNOWN__"


# ── Key extractor (plain function - required for PyFlink key_by callable check)

def _extract_auth_code(value: str) -> str:
    """Return AUTHORIZATION_CODE from a JSON record, or __UNKNOWN__ if absent."""
    try:
        record = json.loads(value)
        tx = record.get("transaction", record)
        code = str(tx.get("AUTHORIZATION_CODE", "")).strip()
        return code if code else _UNKNOWN_KEY
    except Exception:
        return _UNKNOWN_KEY


def _extract_dedup_key(value: str) -> str:
    """Return composite dedup key '{auth_code}|{mti}' for PyFlink key_by.

    Using a composite key of AUTHORIZATION_CODE + message_type (raw MTI) ensures
    that reversal MTIs (1420 = Reversal Request, 1421 = Reversal Repeat,
    1430 = Reversal Advice) reuse the original authorization_code but get their
    own dedup partition — they must not be dropped as duplicates of the original
    authorization.
    """
    try:
        record = json.loads(value)
        tx = record.get("transaction", record)
        auth_code = str(tx.get("AUTHORIZATION_CODE", "")).strip()
        mti       = str(tx.get("MESSAGE_TYPE", "")).strip()
        if not auth_code:
            return _UNKNOWN_KEY
        return f"{auth_code}|{mti}"
    except Exception:
        return _UNKNOWN_KEY


# ── Business logic helpers ─────────────────────────────────────────────────────

def _payment_channel(tx: dict) -> str:
    return "SO_CARTE" if str(tx.get("PRODUCT_CODE", "")).strip() == "6" else "SO_MOBILE"


def _risk_score(tx: dict) -> str:
    """Three-tier risk score evaluated HIGH -> MEDIUM -> LOW.

    Dead branches removed:
    - reject_code non-empty: Job2 Rule ⑥ already filters records with a
      non-empty REJECT_CODE; they never reach Job4.
    - amount == 0: Job2 Rule ② rejects transactions with amount <= 0;
      zero-amount records are routed to the DLQ before reaching Job4.
    """
    matching = str(tx.get("MATCHING_STATUS", "")).strip()

    try:
        amount = float(str(tx.get("TRANSACTION_AMOUNT", 0)).replace(",", "."))
    except (ValueError, TypeError):
        amount = 0.0

    if amount > 10_000:
        return "HIGH"
    if matching not in _VALID_MATCHING_STATUSES:
        return "MEDIUM"
    return "LOW"


# ── KeyedProcessFunction ───────────────────────────────────────────────────────

class OptimizeFn(KeyedProcessFunction):
    """Deduplicate by AUTHORIZATION_CODE and enrich with channel + risk fields.

    State layout
    ------------
    _seen (ValueState[bool])
        Persisted per keyed partition in the Flink state backend.
        True -> this AUTHORIZATION_CODE was already processed; drop.
        None -> first occurrence; process and set True.
    """

    def open(self, runtime_context) -> None:
        self._seen = runtime_context.get_state(
            ValueStateDescriptor("seen-auth-mti", Types.BOOLEAN())
        )
        log.info("OptimizeFn ready (ValueState initialised, composite auth|mti key)")

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context):
        current_key = ctx.get_current_key()

        # Records keyed __UNKNOWN__ are not deduplicated against each other
        # because they lack a real business key. All other records are keyed by
        # composite auth_code|mti so reversals (MTI 1420/1421/1430) sharing the
        # same auth_code get their own partition and pass dedup.
        if current_key != _UNKNOWN_KEY and self._seen.value():
            log.debug("Duplicate AUTHORIZATION_CODE=%s - skipped", current_key)
            return

        if current_key != _UNKNOWN_KEY:
            self._seen.update(True)

        try:
            record = json.loads(value)
            tx = record.get("transaction", {})

            record["payment_channel"] = _payment_channel(tx)
            record["risk_score"]      = _risk_score(tx)
            record["optimized_at"]    = datetime.now(timezone.utc).isoformat()
            record.setdefault("_meta", {})["job"] = "job4-optimize"

            log.debug(
                "Processed AUTHORIZATION_CODE=%s channel=%s risk=%s",
                current_key, record["payment_channel"], record["risk_score"],
            )
            yield json.dumps(record, ensure_ascii=False)

        except Exception as exc:
            log.error("Enrichment error AUTHORIZATION_CODE=%s: %s", current_key, exc)
            raise


# ── MapFunction (MinIO gold write, pass-through) ───────────────────────────────

class GoldMinioFn(MapFunction):
    """Write each optimised record to MinIO gold layer and pass the value through.

    MapFunction is used because PyFlink's SinkFunction is JavaFunctionWrapper
    and requires a Java object in __init__ — it cannot be subclassed in Python.
    The downstream .print() call provides the required terminal Java-backed
    operator that drives Flink's execution graph.
    """

    def open(self, runtime_context) -> None:
        self._client = get_minio_client(
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        )
        ensure_bucket(self._client, MINIO_BUCKET)
        log.info("GoldMinioFn ready (bucket=%s prefix=%s)", MINIO_BUCKET, GOLD_PREFIX)

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            key = write_record(self._client, MINIO_BUCKET, GOLD_PREFIX, record)
            log.debug("gold -> %s/%s", MINIO_BUCKET, key)
        except Exception as exc:
            log.error("MinIO gold write failed: %s", exc)
        return value


# _kafka_sink is imported from common.kafka_utils (shared factory)


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

    # Key by composite AUTHORIZATION_CODE|MTI for stateful deduplication.
    # Using _extract_dedup_key (not _extract_auth_code) so that reversal MTIs
    # (1420, 1421, 1430) sharing the same auth_code get their own key partition.
    keyed = raw.key_by(_extract_dedup_key, key_type=Types.STRING())

    processed = (
        keyed
        .process(OptimizeFn(), output_type=Types.STRING())
        .uid("optimize-fn")
        .name("OptimizeFn")
    )

    (
        processed
        .map(GoldMinioFn(), output_type=Types.STRING())
        .uid("gold-minio-fn")
        .name("GoldMinioFn")
        .sink_to(_kafka_sink(TOPIC_GOLD))
        .uid("payments-gold-sink")
        .name("payments.gold sink")
    )

    log.info(
        "Submitting job: rt-payments-job4-optimization | source=%s | sink=MinIO gold + %s",
        TOPIC_NORMALIZED, TOPIC_GOLD,
    )
    env.execute("rt-payments-job4-optimization")


if __name__ == "__main__":
    main()
