resource "kubernetes_deployment_v1" "minio" {
  metadata {
    name      = "minio-deployment"
    namespace = "minio"
  }

  timeouts {
    create = "15m"
    update = "15m"
    delete = "15m"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "minio"
      }
    }

    strategy {
      type = "Recreate"
    }

    template {
      metadata {
        labels = {
          app = "minio"
        }
      }

      spec {
        container {
          name              = "minio"
          image             = "minio/minio:latest"
          image_pull_policy = "IfNotPresent"

          args = ["server", "/data", "--console-address", ":9001"]

          port {
            container_port = 9000
          }

          port {
            container_port = 9001
          }

          env {
            name  = "MINIO_ROOT_USER"
            value = "minioadmin"
          }

          env {
            name  = "MINIO_ROOT_PASSWORD"
            value = "minioadmin"
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "minio" {
  metadata {
    name      = "minio-service"
    namespace = "minio"
  }

  spec {
    selector = {
      app = "minio"
    }

    port {
      name        = "api"
      port        = 9000
      target_port = 9000
    }

    port {
      name        = "console"
      port        = 9001
      target_port = 9001
    }
  }
}
