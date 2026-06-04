variable "ingestion_namespace"  { default = "ingestion" }
variable "traitement_namespace" { default = "traitement" }
variable "stockage_namespace"   { default = "stockage" }
variable "kafka_version"        { default = "3.8.0" }
variable "kafka_cluster_name"   { default = "payments-cluster" }
variable "kafka_replicas"       { default = 1 }
variable "kafka_partitions"     { default = 1 }
variable "minio_bucket"         { default = "rt-payments" }
variable "minio_access_key"     { default = "minioadmin" }
variable "minio_secret_key"     { default = "minioadmin" }
variable "jobs_image"           { default = "rt-payments-flink-jobs:1.0" }
variable "fernet_key" {
  description = "Fernet key shared by producer (encrypt) and Job 1 (decrypt)"
  sensitive   = true
}
