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

# Attente CRDs Flink — barrière de synchronisation avant stage2
resource "null_resource" "wait_flink_crds" {
  depends_on = [helm_release.flink_operator]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command = <<-SCRIPT
      echo "[CRD wait] Attente des CRDs Flink..."
      for crd in flinkdeployments.flink.apache.org flinksessionjobs.flink.apache.org; do
        echo -n "  $crd ..."
        until kubectl get crd "$crd" &>/dev/null; do echo -n "."; sleep 3; done
        kubectl wait crd "$crd" --for=condition=Established --timeout=120s
        echo " OK"
      done
      echo "[CRD wait] CRDs Flink prêts."
    SCRIPT
  }
}

