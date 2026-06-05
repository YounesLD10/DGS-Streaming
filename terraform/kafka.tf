# ── Strimzi Operator ───────────────────────────────────────────────────────────
resource "helm_release" "strimzi" {
  name            = "strimzi-kafka-operator"
  repository      = "https://strimzi.io/charts/"
  chart           = "strimzi-kafka-operator"
  version         = var.strimzi_version
  namespace       = kubernetes_namespace.kafka.metadata[0].name
  wait            = true
  timeout         = 300
  cleanup_on_fail = true

  set {
    name  = "watchNamespaces"
    value = "{${var.kafka_namespace}}"
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
    value = "384Mi"
  }
  set {
    name  = "resources.limits.cpu"
    value = "500m"
  }
}

# ── KafkaNodePool + Kafka Cluster + Topics ─────────────────────────────────────
# Uses null_resource + local-exec because kubernetes_manifest requires CRDs
# to already exist at plan time, which they don't before Strimzi is installed.
resource "null_resource" "kafka_cluster" {
  depends_on = [helm_release.strimzi]

  triggers = {
    strimzi_version  = var.strimzi_version
    kafka_version    = var.kafka_version
    metadata_version = var.kafka_metadata_version
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail

      echo "[kafka] Waiting for Strimzi CRDs..."
      for i in $(seq 1 40); do
        minikube kubectl -- get crd kafkas.kafka.strimzi.io >/dev/null 2>&1 && break
        sleep 5
      done

      echo "[kafka] Applying KafkaNodePool + Kafka cluster..."
      minikube kubectl -- apply -f - <<'YAML'
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: dual-role
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  replicas: 1
  roles:
    - controller
    - broker
  storage:
    type: ephemeral
  resources:
    requests:
      memory: 512Mi
      cpu: 200m
    limits:
      memory: 768Mi
      cpu: 500m
  jvmOptions:
    -Xms: 128m
    -Xmx: 384m
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: hps-cluster
  namespace: kafka
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: 4.1.0
    metadataVersion: 4.1-IV1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 1
      transaction.state.log.replication.factor: 1
      transaction.state.log.min.isr: 1
  entityOperator:
    topicOperator: {}
    userOperator: {}
YAML

      echo "[kafka] Waiting for Kafka broker pod to appear (up to 5 min)..."
      for i in $(seq 1 60); do
        minikube kubectl -- get pod -n kafka \
          -l strimzi.io/cluster=hps-cluster,strimzi.io/kind=Kafka \
          -o name 2>/dev/null | grep -q pod && break
        sleep 5
      done
      echo "[kafka] Pod found. Waiting for Ready..."
      minikube kubectl -- wait pod -n kafka \
        -l strimzi.io/cluster=hps-cluster,strimzi.io/kind=Kafka \
        --for=condition=Ready --timeout=300s

      echo "[kafka] Waiting for Entity Operator (up to 3 min)..."
      minikube kubectl -- wait deployment/hps-cluster-entity-operator \
        -n kafka --for=condition=Available --timeout=180s

      echo "[kafka] Creating topics..."
      minikube kubectl -- apply -f - <<'YAML'
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "86400000"
    segment.bytes: "67108864"
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments.dlq
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "259200000"
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments.decrypted
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "86400000"
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments.validated
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "86400000"
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments.normalized
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "86400000"
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: payments.gold
  namespace: kafka
  labels:
    strimzi.io/cluster: hps-cluster
spec:
  partitions: 3
  replicas: 1
  config:
    retention.ms: "604800000"
YAML

      echo "[kafka] Waiting for topics..."
      for topic in payments payments.dlq payments.decrypted payments.validated payments.normalized payments.gold; do
        for i in $(seq 1 30); do
          ready=$(minikube kubectl -- get kafkatopic "$topic" -n kafka \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
          [ "$ready" = "True" ] && break
          sleep 5
        done
        echo "[kafka] Topic $topic: ready"
      done

      echo "[kafka] All done!"
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      minikube kubectl -- delete kafkatopic payments payments.dlq -n kafka --ignore-not-found || true
      minikube kubectl -- delete kafka hps-cluster -n kafka --ignore-not-found || true
      minikube kubectl -- delete kafkanodepool dual-role -n kafka --ignore-not-found || true
    EOT
  }
}
