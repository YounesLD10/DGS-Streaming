# Strimzi Operator (Kafka)
resource "helm_release" "strimzi_operator" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = var.strimzi_version
  namespace        = var.ingestion_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.ingestion]

  set {
    name  = "watchNamespaces"
    value = "{${var.ingestion_namespace}}"
  }
  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }

  timeout         = 300
  wait            = true
  wait_for_jobs   = true
}

# Kafka Cluster (KRaft mode — sans ZooKeeper)
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
        version  = "3.7.0"
        replicas = var.kafka_replicas
        listeners = [
          {
            name = "plain"
            port = 9092
            type = "internal"
            tls  = false
          },
          {
            name = "external"
            port = 9094
            type = "nodeport"
            tls  = false
          }
        ]
        config = {
          "offsets.topic.replication.factor"         = "1"
          "transaction.state.log.replication.factor" = "1"
          "transaction.state.log.min.isr"            = "1"
          "default.replication.factor"               = "1"
          "min.insync.replicas"                      = "1"
          "inter.broker.protocol.version"            = "3.7"
        }
        storage = {
          type = "ephemeral"
        }
        resources = {
          requests = { memory = "512Mi", cpu = "250m" }
          limits   = { memory = "1Gi",  cpu = "500m" }
        }
      }
      zookeeper = {
        replicas = 1
        storage  = { type = "ephemeral" }
        resources = {
          requests = { memory = "256Mi", cpu = "100m" }
          limits   = { memory = "512Mi", cpu = "200m" }
        }
      }
      entityOperator = {
        topicOperator = {}
        userOperator  = {}
      }
    }
  }
  depends_on = [helm_release.strimzi_operator]
}

# Topic : payments
resource "kubernetes_manifest" "topic_payments" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = var.topic_payments
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = var.kafka_partitions
      replicas   = 1
      config = {
        "retention.ms"       = "604800000"   # 7 jours
        "cleanup.policy"     = "delete"
        "compression.type"   = "lz4"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}

# Topic : payments.dlq
resource "kubernetes_manifest" "topic_dlq" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = replace(var.topic_dlq, ".", "-")  # K8s name
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = 1
      replicas   = 1
      config = {
        "retention.ms"   = "2592000000"  # 30 jours
        "cleanup.policy" = "delete"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}
