#  MinIO (standalone, mode POC)
resource "helm_release" "minio" {
  name             = "minio"
  repository       = "https://charts.min.io/"
  chart            = "minio"
  namespace        = var.stockage_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.stockage]

  set {
    name  = "mode"
    value = "standalone"
  }
  set {
    name  = "rootUser"
    value = var.minio_access_key
  }
  set {
    name  = "rootPassword"
    value = var.minio_secret_key
  }
  set {
    name  = "persistence.enabled"
    value = "false"     # ephemeral pour le POC
  }
  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }
  set {
    name  = "buckets[0].name"
    value = var.minio_bucket
  }
  set {
    name  = "buckets[0].policy"
    value = "none"
  }
  set {
    name  = "buckets[0].purge"
    value = "false"
  }
  set {
    name  = "service.type"
    value = "ClusterIP"
  }

  timeout = 300
  wait    = true
}

#  Secret MinIO credentials
resource "kubernetes_secret" "minio_creds" {
  metadata {
    name      = "minio-credentials"
    namespace = var.stockage_namespace
  }
  data = {
    access_key = var.minio_access_key
    secret_key = var.minio_secret_key
  }
  type       = "Opaque"
  depends_on = [kubernetes_namespace.stockage]
}
