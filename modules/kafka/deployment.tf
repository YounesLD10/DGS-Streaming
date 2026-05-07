resource "kubernetes_deployment_v1" "kafka" {
  metadata {
    name      = "kafka-deployment"
    namespace = "kafka"
  }

  timeouts {
    create = "15m"
    update = "15m"
    delete = "15m"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "kafka"
      }
    }

    strategy {
      type = "Recreate"
    }

    template {
      metadata {
        labels = {
          app = "kafka"
        }
      }

      spec {
        container {
          name              = "kafka"
          image             = "apache/kafka:3.8.0"
          image_pull_policy = "IfNotPresent"

          port {
            container_port = 9092
          }

          env {
            name  = "KAFKA_NODE_ID"
            value = "1"
          }

          env {
            name  = "KAFKA_PROCESS_ROLES"
            value = "broker,controller"
          }

          env {
            name  = "KAFKA_LISTENERS"
            value = "PLAINTEXT://:9092,CONTROLLER://:9093"
          }

          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://kafka-service.kafka.svc.cluster.local:9092"
          }

          env {
            name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
            value = "CONTROLLER"
          }

          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          }

          env {
            name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
            value = "1@localhost:9093"
          }

          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }

          env {
            name  = "KAFKA_LOG_DIRS"
            value = "/var/lib/kafka/data"
          }
        }
      }
    }
  }
}
