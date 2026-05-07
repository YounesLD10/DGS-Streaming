# 1. Bloquer tout le trafic entrant par défaut dans tous les namespaces
resource "kubernetes_network_policy_v1" "default_deny_all" {
  for_each = toset(["kafka", "minio", "flink"])
  
  metadata {
    name      = "default-deny-all"
    namespace = each.value
  }

  spec {
    pod_selector {}
    policy_types = ["Ingress"]
  }
}

# 2. Autoriser Flink à parler à Kafka (Port 9092)
resource "kubernetes_network_policy_v1" "allow_flink_to_kafka" {
  metadata {
    name      = "allow-flink-to-kafka"
    namespace = "kafka"
  }

  spec {
    pod_selector {
      match_labels = {
        app = "kafka"
      }
    }

    ingress {
      from {
        namespace_selector {
          match_labels = {
            app = "flink"
          }
        }
      }
      ports {
        port     = "9092"
        protocol = "TCP"
      }
    }
    policy_types = ["Ingress"]
  }
}
