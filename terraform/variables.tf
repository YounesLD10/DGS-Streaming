variable "ingestion_namespace"  { default = "ingestion" }
variable "traitement_namespace" { default = "traitement" }
variable "stockage_namespace"   { default = "stockage" }

variable "strimzi_version"         { default = "0.40.0" }
variable "flink_operator_version"  { default = "1.8.0" }
variable "minio_operator_version"  { default = "5.0.15" }

variable "kafka_cluster_name"   { default = "payments-cluster" }
variable "kafka_replicas"       { default = 1 }
variable "kafka_partitions"     { default = 3 }
variable "topic_payments"       { default = "payments" }
variable "topic_dlq"            { default = "payments.dlq" }

variable "minio_bucket"      { default = "rt-payments" }
variable "minio_access_key"  { default = "minioadmin" }
variable "minio_secret_key"  { default = "minioadmin" }

variable "flink_jobmanager_replicas"   { default = 1 }
variable "flink_taskmanager_replicas"  { default = 2 }
