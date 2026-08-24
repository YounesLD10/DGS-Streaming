# prevent_destroy: deleting this namespace cascades to delete the entire
# Kafka cluster and every topic inside it (more catastrophic than even
# null_resource.kafka_cluster's own destroy path) — block it from running
# via taint, trigger change, or `terraform destroy` without a conscious,
# explicit decision (remove this lifecycle block first).
resource "kubernetes_namespace" "kafka" {
  metadata {
    name = var.kafka_namespace
  }

  lifecycle {
    prevent_destroy = true
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
