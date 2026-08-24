variable "kafka_namespace" {
  description = "Kubernetes namespace for Kafka"
  type        = string
  default     = "kafka"
}

variable "flink_namespace" {
  description = "Kubernetes namespace for Flink"
  type        = string
  default     = "flink"
}

variable "minio_namespace" {
  description = "Kubernetes namespace for MinIO"
  type        = string
  default     = "minio"
}

variable "strimzi_version" {
  description = "Strimzi Kafka operator Helm chart version"
  type        = string
  default     = "0.51.0"
}

variable "kafka_version" {
  description = "Apache Kafka version"
  type        = string
  default     = "4.1.0"
}

variable "kafka_metadata_version" {
  description = "Kafka metadata version (KRaft)"
  type        = string
  default     = "4.1-IV1"
}

variable "minio_root_user" {
  description = "MinIO root username"
  type        = string
  default     = "admin"
}

variable "minio_root_password" {
  description = "MinIO root password"
  type        = string
  sensitive   = true
  default     = "admin123"
}

variable "minio_bucket" {
  description = "MinIO bucket name for payment data"
  type        = string
  default     = "rt-payments"
}

variable "kafka_connect_namespace" {
  description = "Kubernetes namespace hosting Kafka Connect and the PostgreSQL data mart (pre-existing, created outside Terraform)"
  type        = string
  default     = "kafka-connect"
}

variable "python_runtime_image" {
  description = "Base image for Terraform-managed Python workloads (kafka-python requires <=3.11; 3.12 breaks its vendored six import)"
  type        = string
  default     = "python:3.11-slim"
}
