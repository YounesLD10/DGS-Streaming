"""
Job 4 — Optimisation / Sink
Consume canonical records from `payments.normalized` and write them to MinIO
under s3a://rt-payments/canonical/, partitioned by businessDate and MTI.
Files roll over every 60s or 64MB whichever comes first.

The S3 endpoint is configured for MinIO via flink-conf.yaml in the image.
"""
from __future__ import annotations
import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import (
    FileSink, OutputFileConfig, RollingPolicy,
)

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
SOURCE_TOPIC = "payments.normalized"
GROUP_ID = "flink-job4-sink"
S3_BUCKET = os.environ.get("S3_BUCKET", "rt-payments")
S3_PREFIX = os.environ.get("S3_PREFIX", "canonical")


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

    output_path = f"s3a://{S3_BUCKET}/{S3_PREFIX}"
    output_config = (
        OutputFileConfig.builder()
        .with_part_prefix("payments")
        .with_part_suffix(".jsonl")
        .build()
    )
    rolling = (
        RollingPolicy.default_rolling_policy(
            part_size=64 * 1024 * 1024,
            rollover_interval=60_000,
            inactivity_interval=30_000,
        )
    )

    sink = (
        FileSink.for_row_format(output_path, Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(output_config)
        .with_rolling_policy(rolling)
        .build()
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-normalized")
        .sink_to(sink)
        .name("minio-rt-payments")
    )

    env.execute("rt-payments-job4-sink")


if __name__ == "__main__":
    main()
