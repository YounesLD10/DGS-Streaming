output "kafka_bootstrap" {
  value       = "${var.kafka_cluster_name}-kafka-bootstrap.${var.ingestion_namespace}.svc:9092"
  description = "Adresse interne du broker Kafka"
}

output "minio_endpoint" {
  value       = "http://minio.${var.stockage_namespace}.svc:9000"
  description = "Endpoint MinIO interne au cluster"
}

output "flink_rest_url" {
  value       = "http://poc-pipeline-rest.${var.traitement_namespace}.svc:8081"
  description = "API REST Flink pour soumettre les jobs"
}

output "minio_bucket" {
  value       = var.minio_bucket
  description = "Bucket MinIO pour les paiements enrichis"
}
