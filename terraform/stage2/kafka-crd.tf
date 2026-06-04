resource "kubernetes_manifest" "kafka_cluster" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "Kafka"
    metadata = {
      name      = var.kafka_cluster_name
      namespace = var.ingestion_namespace
    }
    spec = {
      kafka = {
        version  = var.kafka_version
        replicas = var.kafka_replicas
        listeners = [
          { name = "plain",    port = 9092, type = "internal", tls = false },
          { name = "external", port = 9094, type = "nodeport", tls = false }
        ]
        config = {
          "offsets.topic.replication.factor"         = "1"
          "transaction.state.log.replication.factor" = "1"
          "transaction.state.log.min.isr"            = "1"
          "default.replication.factor"               = "1"
          "min.insync.replicas"                      = "1"

        }
        storage   = { type = "ephemeral" }
        resources = {
          requests = { memory = "512Mi", cpu = "250m" }
          limits   = { memory = "1Gi",  cpu = "500m" }
        }
      }
      zookeeper = {
        replicas  = 1
        storage   = { type = "ephemeral" }
        resources = {
          requests = { memory = "256Mi", cpu = "100m" }
          limits   = { memory = "512Mi", cpu = "200m" }
        }
      }
      entityOperator = { topicOperator = {}, userOperator = {} }
    }
  }
}

# Pipeline topics: ingestion → 4 stages → DLQ
locals {
  pipeline_topics = {
    "payments"            = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.decrypted"  = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.validated"  = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.normalized" = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.dlq"        = { partitions = 1,                    retention_ms = 2592000000 }
  }
}

resource "kubernetes_manifest" "pipeline_topics" {
  for_each = local.pipeline_topics
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      # Resource name must match RFC1123 (no dots) — Strimzi reads spec.topicName for the actual Kafka name.
      name      = replace(each.key, ".", "-")
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      topicName  = each.key
      partitions = each.value.partitions
      replicas   = 1
      config = {
        "retention.ms"     = tostring(each.value.retention_ms)
        "cleanup.policy"   = "delete"
        "compression.type" = "lz4"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}
