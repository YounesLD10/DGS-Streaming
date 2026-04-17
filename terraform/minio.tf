# ── MinIO Object Storage ───────────────────────────────────────────────────────
resource "helm_release" "minio" {
  name            = "minio"
  repository      = "https://charts.min.io/"
  chart           = "minio"
  namespace       = kubernetes_namespace.minio.metadata[0].name
  wait            = true
  timeout         = 300
  cleanup_on_fail = true

  set {
    name  = "mode"
    value = "standalone"
  }
  set {
    name  = "rootUser"
    value = var.minio_root_user
  }
  set {
    name  = "rootPassword"
    value = var.minio_root_password
  }
  set {
    name  = "persistence.enabled"
    value = "false"
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
    name  = "resources.limits.memory"
    value = "512Mi"
  }
  set {
    name  = "resources.limits.cpu"
    value = "500m"
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
}
