# Flink Kubernetes Operator
resource "helm_release" "flink_operator" {
  name             = "flink-kubernetes-operator"
  repository       = "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${var.flink_operator_version}/"
  chart            = "flink-kubernetes-operator"
  version          = var.flink_operator_version
  namespace        = var.traitement_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.traitement]

  set {
    name  = "webhook.create"
    value = "false"
  }
  set {
    name  = "operatorPod.resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "operatorPod.resources.requests.cpu"
    value = "100m"
  }

  timeout = 300
  wait    = true
}

# ConfigMap Flink — paramètres communs
resource "kubernetes_config_map" "flink_config" {
  metadata {
    name      = "flink-pipeline-config"
    namespace = var.traitement_namespace
  }
  data = {
    "kafka.bootstrap.servers" = "payments-cluster-kafka-bootstrap.ingestion.svc:9092"
    "kafka.topic.input"       = "payments"
    "kafka.topic.dlq"         = "payments.dlq"
    "minio.endpoint"          = "http://minio.stockage.svc:9000"
    "minio.bucket"            = "rt-payments"
    "flink.checkpointing.interval" = "60000"
    "flink.state.backend"          = "rocksdb"
  }
  depends_on = [kubernetes_namespace.traitement]
}

# Secret MinIO credentials (utilisé par Flink)
resource "kubernetes_secret" "minio_creds_flink" {
  metadata {
    name      = "minio-credentials"
    namespace = var.traitement_namespace
  }
  data = {
    access_key = var.minio_access_key
    secret_key = var.minio_secret_key
  }
  type       = "Opaque"
  depends_on = [kubernetes_namespace.traitement]
}

# FlinkDeployment : 1 JobManager + 2 TaskManagers
resource "kubernetes_manifest" "flink_deployment" {
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata = {
      name      = "poc-pipeline"
      namespace = var.traitement_namespace
    }
    spec = {
      image          = "flink:1.18-scala_2.12"
      flinkVersion   = "v1_18"
      imagePullPolicy = "IfNotPresent"
      serviceAccount = "flink"

      flinkConfiguration = {
        "taskmanager.numberOfTaskSlots" = "4"
        "state.backend"                 = "rocksdb"
        "state.checkpoints.dir"         = "file:///tmp/flink-checkpoints"
        "execution.checkpointing.interval"             = "60s"
        "execution.checkpointing.mode"                 = "EXACTLY_ONCE"
        "execution.checkpointing.min-pause"            = "30s"
        "restart-strategy"                             = "exponential-delay"
        "restart-strategy.exponential-delay.initial-backoff" = "1s"
        "restart-strategy.exponential-delay.max-backoff"     = "5min"
      }

      jobManager = {
        resource = {
          memory = "1024m"
          cpu    = 0.5
        }
        replicas = 1
      }

      taskManager = {
        resource = {
          memory = "1024m"
          cpu    = 1.0
        }
        replicas = 2
      }

      # Pas de job embarqué au démarrage (session cluster)
      # Les 4 jobs seront soumis séparément via Flink REST API
      mode = "standalone"
    }
  }
  depends_on = [helm_release.flink_operator]
}
