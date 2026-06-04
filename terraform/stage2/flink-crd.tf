# Same Fernet key in both namespaces (producer reads from ingestion, Job 1 from traitement)
resource "kubernetes_secret" "fernet_traitement" {
  metadata {
    name      = "fernet-key"
    namespace = var.traitement_namespace
  }
  type = "Opaque"
  data = { key = var.fernet_key }
}

resource "kubernetes_secret" "fernet_ingestion" {
  metadata {
    name      = "fernet-key"
    namespace = var.ingestion_namespace
  }
  type = "Opaque"
  data = { key = var.fernet_key }
}

locals {
  bootstrap = "${var.kafka_cluster_name}-kafka-bootstrap.${var.ingestion_namespace}.svc:9092"
  jobs = {
    "job1-decryption"    = { script = "job1_decryption.py" }
    "job2-validation"    = { script = "job2_validation.py" }
    "job3-normalization" = { script = "job3_normalization.py" }
    "job4-sink"          = { script = "job4_sink.py" }
  }
  flink_conf = {
    "taskmanager.numberOfTaskSlots"                      = "1"
    "state.backend"                                      = "hashmap"
    "state.checkpoints.dir"                              = "file:///tmp/flink-checkpoints"
    "execution.checkpointing.interval"                   = "120s"
    "execution.checkpointing.mode"                       = "AT_LEAST_ONCE"
    "execution.checkpointing.min-pause"                  = "60s"
    "restart-strategy"                                   = "exponential-delay"
    "restart-strategy.exponential-delay.initial-backoff" = "5s"
    "restart-strategy.exponential-delay.max-backoff"     = "5min"
    # Reduce JVM overhead so 768m JM and 640m TM pass Flink memory validation.
    # Default metaspace=256m costs too much; 128m is sufficient for PyFlink.
    # Default overhead.min=192m leaves only 64m Flink memory in a 512m container.
    "jobmanager.memory.jvm-metaspace.size"               = "128mb"
    "jobmanager.memory.jvm-overhead.min"                 = "64mb"
    "taskmanager.memory.jvm-metaspace.size"              = "128mb"
    "taskmanager.memory.jvm-overhead.min"                = "64mb"
    # Reduce managed-memory fraction (Python gets 20% instead of 40%).
    # A simple POC streaming job needs far less than the default 40%.
    "taskmanager.memory.managed.fraction"                = "0.2"
    "s3.endpoint"                                        = "http://minio.${var.stockage_namespace}.svc:9000"
    "s3.path.style.access"                               = "true"
    "s3.access-key"                                      = var.minio_access_key
    "s3.secret-key"                                      = var.minio_secret_key
  }
}

resource "kubernetes_manifest" "flink_jobs" {
  for_each = local.jobs
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata = {
      name      = each.key
      namespace = var.traitement_namespace
      labels    = {
        "app.kubernetes.io/part-of"   = "rt-payments"
        "app.kubernetes.io/component" = each.key
      }
    }
    spec = {
      image           = var.jobs_image
      imagePullPolicy = "Never"
      flinkVersion    = "v1_18"
      serviceAccount  = "flink"
      flinkConfiguration = local.flink_conf
      # JM 768m: metaspace(128)+overhead(77)=205 → Flink memory=563m ✓
      # TM 640m: metaspace(128)+overhead(64)=192 → Flink memory=448m ✓
      jobManager  = { resource = { memory = "768m", cpu = 0.25 }, replicas = 1 }
      taskManager = { resource = { memory = "640m", cpu = 0.5  }, replicas = 1 }
      podTemplate = {
        spec = {
          containers = [{
            name = "flink-main-container"
            env = [
              { name = "KAFKA_BOOTSTRAP", value = local.bootstrap },
              { name  = "FERNET_KEY"
                valueFrom = { secretKeyRef = { name = "fernet-key", key = "key" } }
              },
              { name = "S3_BUCKET", value = var.minio_bucket },
            ]
          }]
        }
      }
      job = {
        jarURI      = "local:///opt/flink/python-driver/flink-python.jar"
        entryClass  = "org.apache.flink.client.python.PythonDriver"
        args        = ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/jobs/${each.value.script}"]
        parallelism = 1
        upgradeMode = "stateless"
      }
    }
  }
  depends_on = [kubernetes_secret.fernet_traitement]
}
