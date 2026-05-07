resource "kubernetes_deployment_v1" "flink_jobmanager" {
  metadata {
    name      = "flink-jobmanager"
    namespace = "flink"
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
        app       = "flink"
        component = "jobmanager"
      }
    }

    strategy {
      type = "Recreate"
    }

    template {
      metadata {
        labels = {
          app       = "flink"
          component = "jobmanager"
        }
      }

      spec {
        container {
          name              = "jobmanager"
          image             = "apache/flink:1.18.1"
          image_pull_policy = "IfNotPresent"

          args = ["jobmanager"]

          port {
            container_port = 6123
          }

          port {
            container_port = 8081
          }

          env {
            name  = "JOB_MANAGER_RPC_ADDRESS"
            value = "flink-jobmanager-service"
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "flink_jobmanager" {
  metadata {
    name      = "flink-jobmanager-service"
    namespace = "flink"
  }

  spec {
    selector = {
      app       = "flink"
      component = "jobmanager"
    }

    port {
      name        = "rpc"
      port        = 6123
      target_port = 6123
    }

    port {
      name        = "ui"
      port        = 8081
      target_port = 8081
    }
  }
}

resource "kubernetes_deployment_v1" "flink_taskmanager" {
  metadata {
    name      = "flink-taskmanager"
    namespace = "flink"
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
        app       = "flink"
        component = "taskmanager"
      }
    }

    strategy {
      type = "Recreate"
    }

    template {
      metadata {
        labels = {
          app       = "flink"
          component = "taskmanager"
        }
      }

      spec {
        container {
          name              = "taskmanager"
          image             = "apache/flink:1.18.1"
          image_pull_policy = "IfNotPresent"

          args = ["taskmanager"]

          env {
            name  = "JOB_MANAGER_RPC_ADDRESS"
            value = "flink-jobmanager-service"
          }
        }
      }
    }
  }
}
