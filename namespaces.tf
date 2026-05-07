resource "kubernetes_namespace_v1" "kafka" {
  metadata {
    name = "kafka"
  }
}

resource "kubernetes_namespace_v1" "flink" {
  metadata {
    name = "flink"
  }
}

resource "kubernetes_namespace_v1" "minio" {
  metadata {
    name = "minio"
  }
}
