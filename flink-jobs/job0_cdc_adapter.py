"""
Job 0 — CDC Adapter
Consumes Debezium CDC events from `cdc.public.powercard_operations`,
extracts INSERT/UPDATE rows, and emits pipeline envelopes to
`payments.decrypted` — bypassing Fernet decryption since the data
is already plaintext from PostgreSQL.

Debezium envelope (schemas disabled — flat format):
  {"before": null, "after": {row_lowercase}, "op": "c"|"u"|"r"}
Debezium envelope (schemas enabled — wrapped format):
  {"payload": {"before": null, "after": {row_lowercase}, "op": "c"|"u"|"r"}}

Pipeline envelope emitted:
  {"eventId": "...", "source": "postgresql-cdc", "payload": {ROW_UPPERCASE},
   "_processing": {"cdcOp": "c", "cdcIngestedAt": 1234}}

The `after` fields use lowercase column names (PostgreSQL convention).
This job uppercases them to match the downstream pipeline expectations
(job2_validation, job3_normalization read e.g. p.get("MESSAGE_TYPE")).
"""
from __future__ import annotations
import json
import os
import time
import uuid

from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema, DeliveryGuarantee,
)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SOURCE_TOPIC = os.environ.get("CDC_TOPIC", "cdc.public.powercard_operations")
SINK_TOPIC = "payments.decrypted"
GROUP_ID = "flink-job0-cdc-adapter"

_SKIP_COLS = {"id", "ingested_at"}


class CdcAdapterFn(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            msg = json.loads(value)
            # schemas disabled → op/after at root; schemas enabled → inside "payload"
            envelope = msg.get("payload") or msg
            op = envelope.get("op", "")
            if op not in ("c", "u", "r"):
                return  # skip deletes and tombstones
            after = envelope.get("after")
            if not after:
                return
            row = {
                k.upper(): (str(v) if v is not None else None)
                for k, v in after.items()
                if k not in _SKIP_COLS
            }
            envelope = {
                "eventId": str(uuid.uuid4()),
                "source": "postgresql-cdc",
                "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "payload": row,
                "_processing": {
                    "cdcOp": op,
                    "cdcIngestedAt": int(time.time() * 1000),
                },
            }
            yield json.dumps(envelope, ensure_ascii=False)
        except Exception:
            return


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

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-cdc")
        .process(CdcAdapterFn(), output_type=Types.STRING())
        .sink_to(sink)
        .name("kafka-payments.decrypted")
    )

    env.execute("rt-payments-job0-cdc-adapter")


if __name__ == "__main__":
    main()
