"""
Job 3 — Normalisation
=====================
Source  : Kafka 'payments.validated'
Process : NormalizeFn (ProcessFunction)
            • Convert all date fields (DD/MM/YYYY HH:MM) → ISO 8601 UTC
              Fields: TRANSACTION_LOCAL_DATE, TRANSMISSION_DATE_AND_TIME,
                      RESPONSE_DATE_AND_TIME, CAPTURE_DATE, BUSINESS_DATE,
                      CONVERSION_RATE_DATE, ISS_SETTLEMENT_DATE,
                      ACQ_SETTLEMENT_DATE
            • Convert scientific-notation / comma-decimal amounts → float
              (e.g. "0,11E+02" → 11.0)
            • Convert TRANSACTION_AMOUNT → float
            • Attach metadata: processed_at, source_system, pipeline_version
Sink    : MinIO  rt-payments/silver/   (SilverMinioFn MapFunction, pass-through)
          Kafka  payments.normalized

Architectural notes
-------------------
* Date parsing tries multiple formats and falls back to the original string
  rather than dropping the record — a normalisation failure should not
  invalidate an already-validated payment.
* Scientific notation amounts use Python's own float() after replacing
  comma with period, which naturally handles all exponent formats.
* TRANSACTION_AMOUNT is also normalised here (validated as numeric in
  Job 2, but still stored as string in the decrypted payload).
* Silver objects in MinIO are written per-record via the MinIO Python SDK
  inside a pass-through MapFunction, matching the specified architecture.
"""

import json
import logging
import os
import re
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
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction, ProcessFunction

from common.config import (
    CHECKPOINT_INTERVAL_MS,
    KAFKA_BOOTSTRAP,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PIPELINE_VERSION,
    SOURCE_SYSTEM,
    TOPIC_DLQ,
    TOPIC_NORMALIZED,
    TOPIC_VALIDATED,
)
from common.iso_standards import (
    ISO8583_MTI,
    card_scheme,
    currency_alpha,
    currency_minor_units,
    mcc_description,
)
from common.minio_sink import ensure_bucket, get_minio_client, write_record
from common.kafka_utils import kafka_sink as _kafka_sink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [job3-normalize] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

DLQ_TAG = OutputTag("dlq-normalize", Types.STRING())

SILVER_PREFIX = "silver"

# All date fields present in an HPS SWAM transaction record
DATE_FIELDS = [
    "TRANSACTION_LOCAL_DATE",
    "TRANSMISSION_DATE_AND_TIME",
    "RESPONSE_DATE_AND_TIME",
    "CAPTURE_DATE",
    "BUSINESS_DATE",
    "CONVERSION_RATE_DATE",
    "ISS_SETTLEMENT_DATE",
    "ACQ_SETTLEMENT_DATE",
]

# Formats attempted in order; first match wins
_DATE_FORMATS = [
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]

# Matches "0,11E+02", "1,5e3", "700", "1.5", "-3,14E-01" etc.
_SCI_PATTERN = re.compile(r"^-?[\d]+(?:[,.][\d]+)?(?:[eE][+\-]?\d+)?$")


# ── Normalisation helpers ──────────────────────────────────────────────────────

def _parse_date(raw) -> str | None:
    """Return ISO 8601 UTC string or the original value if unparseable."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    # Return as-is rather than dropping — do not invalidate the record
    return s


def _parse_amount(raw) -> float | None:
    """Parse numeric strings including comma-decimal scientific notation.

    Examples
    --------
    "700"       → 700.0
    "0,11E+02"  → 11.0
    "1.50"      → 1.5
    ""          → None
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if not _SCI_PATTERN.match(s):
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _normalize_transaction(tx: dict) -> dict:
    """Return a new dict with dates and amounts normalised."""
    out = dict(tx)

    for field in DATE_FIELDS:
        if field in out:
            out[field] = _parse_date(out[field])

    if "TRANSACTION_AMOUNT" in out:
        normalised = _parse_amount(out["TRANSACTION_AMOUNT"])
        if normalised is not None:
            out["TRANSACTION_AMOUNT"] = normalised

    return out


# ── ProcessFunction ────────────────────────────────────────────────────────────

class NormalizeFn(ProcessFunction):
    """Normalise dates, amounts, and attach pipeline metadata."""

    def open(self, runtime_context) -> None:
        log.info("NormalizeFn ready")

    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            record = json.loads(value)
            tx = record.get("transaction", {})

            record["transaction"]      = _normalize_transaction(tx)
            record["processed_at"]     = datetime.now(timezone.utc).isoformat()
            record["source_system"]    = SOURCE_SYSTEM
            record["pipeline_version"] = PIPELINE_VERSION
            record.setdefault("_meta", {})["job"] = "job3-normalize"

            pan      = str(tx.get("CARD_NUMBER", "")).strip()
            mcc      = str(tx.get("CARD_ACCEPTOR_ACTIVITY", "")).strip()
            mti      = str(tx.get("MESSAGE_TYPE", "")).strip()
            currency = str(tx.get("TRANSACTION_CURRENCY", "")).strip()

            record["card_scheme"]          = card_scheme(pan)
            record["mcc_description"]      = mcc_description(mcc)
            record["mti_name"]             = ISO8583_MTI.get(mti)
            record["currency_alpha"]       = currency_alpha(currency)
            record["currency_minor_units"] = currency_minor_units(currency)

            yield json.dumps(record, ensure_ascii=False)

        except Exception as exc:
            event_id = record.get("eventId", "unknown") if isinstance(record, dict) else "parse-error"
            log.error("Normalisation error eventId=%s: %s", event_id, exc)
            dlq = {
                "raw": value,
                "error_type": "NORMALISATION_ERROR",
                "error": str(exc),
                "_meta": {
                    "errorAt": datetime.now(timezone.utc).isoformat(),
                    "job": "job3-normalize",
                    "eventId": event_id,
                }
            }
            yield DLQ_TAG, json.dumps(dlq, ensure_ascii=False)


# ── MapFunction (MinIO silver write, pass-through) ─────────────────────────────

class SilverMinioFn(MapFunction):
    """Write each normalised record to MinIO silver layer.

    Mirrors BronzeMinioFn in Job 1 — MinIO failures are logged but
    do not interrupt the main pipeline in the PoC environment.
    """

    def open(self, runtime_context) -> None:
        self._client = get_minio_client(
            MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        )
        ensure_bucket(self._client, MINIO_BUCKET)
        log.info("SilverMinioFn: MinIO client ready (bucket=%s)", MINIO_BUCKET)

    def map(self, value: str) -> str:
        try:
            record = json.loads(value)
            key = write_record(self._client, MINIO_BUCKET, SILVER_PREFIX, record)
            log.debug("silver → %s/%s", MINIO_BUCKET, key)
        except Exception as exc:
            log.error("MinIO silver write failed: %s", exc)
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
        .set_topics(TOPIC_VALIDATED)
        .set_group_id("flink-job3-normalize")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "payments-validated-source",
    )

    processed = (
        raw
        .process(NormalizeFn(), output_type=Types.STRING())
        .uid("normalize-fn")
        .name("NormalizeFn")
    )

    # Route DLQ (normalisation errors) to payments.dlq
    dlq_stream = processed.get_side_output(DLQ_TAG)
    (
        dlq_stream
        .sink_to(_kafka_sink(TOPIC_DLQ))
        .uid("dlq-normalize-sink")
        .name("payments.dlq sink (normalization errors)")
    )

    # Write normalised records to MinIO silver, then forward to Kafka
    (
        processed
        .map(SilverMinioFn(), output_type=Types.STRING())
        .uid("silver-minio-fn")
        .name("SilverMinioFn")
        .sink_to(_kafka_sink(TOPIC_NORMALIZED))
        .uid("payments-normalized-sink")
        .name("payments.normalized sink")
    )

    log.info(
        "Submitting job: rt-payments-job3-normalization | "
        "source=%s | out=%s",
        TOPIC_VALIDATED, TOPIC_NORMALIZED,
    )
    env.execute("rt-payments-job3-normalization")


if __name__ == "__main__":
    main()
