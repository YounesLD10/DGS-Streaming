output "kafka_namespace" {
  value = kubernetes_namespace.kafka.metadata[0].name
}

output "flink_namespace" {
  value = kubernetes_namespace.flink.metadata[0].name
}

output "minio_namespace" {
  value = kubernetes_namespace.minio.metadata[0].name
}

output "kafka_bootstrap" {
  value       = "hps-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
  description = "Kafka bootstrap server address (internal)"
}

output "minio_endpoint" {
  value       = "http://minio.minio.svc.cluster.local:9000"
  description = "MinIO S3-compatible endpoint (internal)"
}

output "minio_bucket" {
  value = var.minio_bucket
}

output "flink_rest_endpoint" {
  value       = "http://flink-jobmanager.flink.svc.cluster.local:8081"
  description = "Flink REST / UI endpoint (internal)"
}
