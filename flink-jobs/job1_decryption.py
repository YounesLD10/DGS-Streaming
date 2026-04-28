"""
Job 1 — Décryption
Consume encrypted envelopes from `payments`, Fernet-decrypt the payload,
republish the cleartext envelope to `payments.decrypted`.
Failures (bad token, parse errors) → `payments.dlq` with reject reason.
"""
from __future__ import annotations
import json
import logging
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
from common.crypto import decrypt_dict  # noqa: E402

LOG = logging.getLogger("job1.decryption")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SOURCE_TOPIC = "payments"
SINK_TOPIC = "payments.decrypted"
DLQ_TOPIC = "payments.dlq"
GROUP_ID = "flink-job1-decryption"

DLQ_TAG = OutputTag("dlq", Types.STRING())


class DecryptFn(ProcessFunction):
    def process_element(self, value, ctx):
        try:
            envelope = json.loads(value)
            token = envelope.get("encrypted_payload")
            if not token:
                raise ValueError("envelope missing encrypted_payload")
            envelope["payload"] = decrypt_dict(token)
            envelope.pop("encrypted_payload", None)
            envelope.setdefault("_processing", {})["decryptedAt"] = int(time.time() * 1000)
            yield json.dumps(envelope, ensure_ascii=False)
        except Exception as exc:
            err = {
                "stage": "decryption",
                "errorCode": "DECRYPT_FAILED",
                "errorMessage": f"{type(exc).__name__}: {exc}",
                "originalMessage": value,
                "timestamp": int(time.time() * 1000),
            }
            yield DLQ_TAG, json.dumps(err, ensure_ascii=False)


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

    out_sink = (
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

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    src = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-payments")
    main_stream = src.process(DecryptFn(), output_type=Types.STRING())
    main_stream.sink_to(out_sink).name("kafka-payments.decrypted")
    main_stream.get_side_output(DLQ_TAG).sink_to(dlq_sink).name("kafka-dlq")

    env.execute("rt-payments-job1-decryption")


if __name__ == "__main__":
    main()
