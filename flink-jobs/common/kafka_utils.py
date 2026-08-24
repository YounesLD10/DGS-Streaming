"""
Shared Kafka utilities for all SWAM pipeline PyFlink jobs.

Provides a common KafkaSink factory to avoid code duplication across
job1_decrypt.py, job2_validate.py, job3_normalize.py, job4_optimize.py.
"""

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaRecordSerializationSchema,
    KafkaSink,
)

from common.config import KAFKA_BOOTSTRAP


def kafka_sink(topic: str) -> KafkaSink:
    """Build an AT_LEAST_ONCE KafkaSink for the given topic.

    This is a module-level factory shared by all pipeline jobs.
    DeliveryGuarantee.AT_LEAST_ONCE is used in conjunction with
    EXACTLY_ONCE checkpointing at the job level.
    """
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
