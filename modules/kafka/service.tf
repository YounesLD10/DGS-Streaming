resource "kubernetes_service_v1" "kafka" {
  metadata {
    name      = "kafka-service"
    namespace = "kafka"
  }
  spec {
    selector = {
      app = "kafka"
    }
    port {
      port        = 9092
      target_port = 9092
    }
    type = "ClusterIP"
  }
}