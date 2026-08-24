"""
Environment-driven configuration for all SWAM pipeline jobs.
All values come from env vars; defaults point to in-cluster endpoints.
No secrets are hardcoded — FERNET_KEY has no default and the job
validates its presence at startup.
"""
import os

# ── Kafka ──────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP: str = os.getenv(
    "KAFKA_BOOTSTRAP",
    "hps-cluster-kafka-bootstrap.kafka.svc:9092",
)

# ── MinIO ──────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "http://minio.minio.svc:9000")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "admin")
# Populated from minio-credentials K8s Secret (MINIO_SECRET_KEY env var) — no hardcoded default
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET: str = os.getenv("MINIO_BUCKET", "rt-payments")

# ── Cryptography ───────────────────────────────────────────────────────────────
# Must be set in the execution environment; no fallback by design.
FERNET_KEY: str = os.getenv("FERNET_KEY", "")

# ── Flink checkpointing ────────────────────────────────────────────────────────
CHECKPOINT_INTERVAL_MS: int = int(os.getenv("CHECKPOINT_INTERVAL_MS", "30000"))

# ── Pipeline metadata ──────────────────────────────────────────────────────────
SOURCE_SYSTEM: str = "SWAM"
PIPELINE_VERSION: str = "1.0.0"

# ── Kafka topic names ──────────────────────────────────────────────────────────
TOPIC_PAYMENTS: str = "payments"
TOPIC_DECRYPTED: str = "payments.decrypted"
TOPIC_VALIDATED: str = "payments.validated"
TOPIC_NORMALIZED: str = "payments.normalized"
TOPIC_DLQ: str = "payments.dlq"
TOPIC_GOLD: str = os.getenv("TOPIC_GOLD", "payments.gold")
