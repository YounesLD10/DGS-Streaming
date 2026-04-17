resource "kubernetes_namespace" "kafka" {
  metadata {
    name = var.kafka_namespace
  }
}

resource "kubernetes_namespace" "flink" {
  metadata {
    name = var.flink_namespace
  }
}

resource "kubernetes_namespace" "minio" {
  metadata {
    name = var.minio_namespace
  }
}
