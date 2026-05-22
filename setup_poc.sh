set -euo pipefail

# Couleurs
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'

log()     { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERR]${NC}   $*" >&2; exit 1; }
step()    { echo -e "\n${CYAN}══════════════════════════════════════════${NC}"; \
            echo -e "${CYAN}  $*${NC}"; \
            echo -e "${CYAN}══════════════════════════════════════════${NC}"; }

# Configuration
MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-9216}"
MINIKUBE_DISK="${MINIKUBE_DISK:-30g}"
MINIKUBE_DRIVER="${MINIKUBE_DRIVER:-docker}"
MINIKUBE_K8S_VERSION="${MINIKUBE_K8S_VERSION:-v1.31.4}"

KAFKA_NAMESPACE="ingestion"
FLINK_NAMESPACE="traitement"
MINIO_NAMESPACE="stockage"

STRIMZI_VERSION="0.43.0"
KAFKA_VERSION="3.8.0"
FLINK_OPERATOR_VERSION="1.10.0"
MINIO_CHART_VERSION="5.4.0"

KAFKA_CLUSTER_NAME="payments-cluster"
KAFKA_TOPIC_PAYMENTS="payments"
KAFKA_TOPIC_DLQ="payments.dlq"
KAFKA_REPLICAS=1          # POC : 1 broker suffit
KAFKA_PARTITIONS=1

MINIO_BUCKET="rt-payments"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

FLINK_JOBMANAGER_REPLICAS=1
FLINK_TASKMANAGER_REPLICAS=2

# Image baked locally inside the minikube docker daemon (imagePullPolicy=Never)
JOBS_IMAGE="${JOBS_IMAGE:-rt-payments-flink-jobs:1.0}"

TERRAFORM_DIR="$(pwd)/terraform"
K8S_MANIFESTS_DIR="$(pwd)/k8s"
SCRIPTS_DIR="$(pwd)/scripts"
JOBS_DIR="$(pwd)/flink-jobs"
PRODUCER_DIR="$(pwd)/producer"
FERNET_KEY_FILE="$(pwd)/.fernet.key"

# CDC — PostgreSQL + Debezium Connect
PG_NAMESPACE="$KAFKA_NAMESPACE"        # co-déployé dans ingestion pour simplifier
PG_DB="${PG_DB:-powercard}"
PG_USER="${PG_USER:-postgres}"
PG_PASSWORD="${PG_PASSWORD:-postgres}"
DEBEZIUM_USER="${DEBEZIUM_USER:-debezium}"
DEBEZIUM_PASSWORD="${DEBEZIUM_PASSWORD:-debezium}"
DEBEZIUM_CONNECT_IMAGE="${DEBEZIUM_CONNECT_IMAGE:-quay.io/debezium/connect:2.7.0.Final}"

# Vérification des prérequis 
check_prerequisites() {
  step "Vérification des prérequis"
  local missing=()
  for tool in minikube kubectl helm terraform jq curl; do
    if command -v "$tool" &>/dev/null; then
      success "$tool : $(command -v $tool)"
    else
      missing+=("$tool")
      warn "$tool : NON TROUVÉ"
    fi
  done
  [[ ${#missing[@]} -gt 0 ]] && error "Outils manquants : ${missing[*]}"
  success "Tous les prérequis sont satisfaits"
}

# Minikube — démarre le cluster en détectant et récupérant les états corrompus
start_minikube() {
  step "Démarrage de Minikube"

  # ── Cluster déjà en cours et API server sain → vérifier la version K8s ─────
  if minikube status 2>/dev/null | grep -q "Running"; then
    if kubectl cluster-info &>/dev/null; then
      # Vérifier si la version K8s correspond à celle requise
      CURRENT_K8S=$(kubectl version --short 2>/dev/null | grep "Server Version" | grep -oP 'v[\d.]+' || \
                    kubectl version -o json 2>/dev/null | grep -o '"gitVersion":"v[^"]*"' | head -1 | grep -oP 'v[\d.]+' || echo "unknown")
      REQUIRED_K8S="${MINIKUBE_K8S_VERSION}"
      if [[ -n "$REQUIRED_K8S" && "$CURRENT_K8S" != "$REQUIRED_K8S" ]]; then
        warn "Version K8s incompatible : cluster=$CURRENT_K8S, requis=$REQUIRED_K8S"
        warn "Strimzi 0.43 / fabric8 6.13 crashe sur K8s 1.32+ (champ 'emulationMajor' inconnu)"
        warn "Purge du cluster pour démarrer avec $REQUIRED_K8S..."
        _minikube_purge
      else
        warn "Minikube est déjà en cours d'exécution et sain — rien à faire"
        return
      fi
    else
      warn "Minikube reported Running mais l\'API server ne répond pas → purge forcée"
      _minikube_purge
    fi
  fi

  # ── Premier essai de démarrage ───────────────────────────────────────────
  log "Lancement de Minikube (CPUs=$MINIKUBE_CPUS, RAM=${MINIKUBE_MEMORY}MB, Disk=$MINIKUBE_DISK)"
  if ! _minikube_start; then
    warn "Démarrage échoué (cluster corrompu ou cgroup error) → purge et nouvelle tentative"
    _minikube_purge
    log "Nouvelle tentative de démarrage Minikube..."
    _minikube_start || error "Impossible de démarrer Minikube après purge. Vérifiez Docker/systemd."
  fi

  success "Minikube démarré"
  kubectl cluster-info
}

# Encapsule minikube start — retourne le code de sortie sans tuer le script (pas de set -e)
_minikube_start() {
  minikube start \
    --driver="$MINIKUBE_DRIVER" \
    --cpus="$MINIKUBE_CPUS" \
    --memory="$MINIKUBE_MEMORY" \
    --disk-size="$MINIKUBE_DISK" \
    --kubernetes-version="$MINIKUBE_K8S_VERSION" \
    --addons=ingress,metrics-server \
    --embed-certs
}

# Supprime entièrement le cluster Minikube corrompu
_minikube_purge() {
  warn "Suppression du cluster Minikube existant (minikube delete --all --purge)..."
  minikube stop   2>/dev/null || true
  minikube delete --all --purge 2>/dev/null || true
  sleep 5   # laisser Docker libérer les ressources cgroup
  success "Cluster Minikube supprimé — repartir de zéro"
}

# Génération des fichiers Terraform
generate_terraform_files() {
  step "Génération des fichiers Terraform"
  mkdir -p "$TERRAFORM_DIR"

  # main.tf
  cat > "$TERRAFORM_DIR/main.tf" << 'EOF'
terraform {
  required_version = ">= 1.7"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.27"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.13"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

# Providers
provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "minikube"
}

provider "helm" {
  kubernetes {
    config_path    = "~/.kube/config"
    config_context = "minikube"
  }
}
EOF

  # variables.tf
  cat > "$TERRAFORM_DIR/variables.tf" << EOF
variable "ingestion_namespace"  { default = "$KAFKA_NAMESPACE" }
variable "traitement_namespace" { default = "$FLINK_NAMESPACE" }
variable "stockage_namespace"   { default = "$MINIO_NAMESPACE" }

variable "strimzi_version"         { default = "$STRIMZI_VERSION" }
variable "flink_operator_version"  { default = "$FLINK_OPERATOR_VERSION" }
variable "minio_chart_version"    { default = "$MINIO_CHART_VERSION" }

variable "kafka_version"       { default = "$KAFKA_VERSION" }
variable "kafka_cluster_name"   { default = "$KAFKA_CLUSTER_NAME" }
variable "kafka_replicas"       { default = $KAFKA_REPLICAS }
variable "kafka_partitions"     { default = $KAFKA_PARTITIONS }
variable "topic_payments"       { default = "$KAFKA_TOPIC_PAYMENTS" }
variable "topic_dlq"            { default = "$KAFKA_TOPIC_DLQ" }

variable "minio_bucket"      { default = "$MINIO_BUCKET" }
variable "minio_access_key"  { default = "$MINIO_ACCESS_KEY" }
variable "minio_secret_key"  { default = "$MINIO_SECRET_KEY" }

variable "flink_jobmanager_replicas"   { default = $FLINK_JOBMANAGER_REPLICAS }
variable "flink_taskmanager_replicas"  { default = $FLINK_TASKMANAGER_REPLICAS }
EOF

  # namespaces.tf
  cat > "$TERRAFORM_DIR/namespaces.tf" << 'EOF'
# Namespaces
resource "kubernetes_namespace" "ingestion" {
  metadata {
    name   = var.ingestion_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}

resource "kubernetes_namespace" "traitement" {
  metadata {
    name   = var.traitement_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}

resource "kubernetes_namespace" "stockage" {
  metadata {
    name   = var.stockage_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}
EOF

  # kafka.tf — helm_release natif Terraform (plus de null_resource+local-exec)
  cat > "$TERRAFORM_DIR/kafka.tf" << 'EOF'
resource "helm_release" "strimzi_operator" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = var.strimzi_version
  namespace        = var.ingestion_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.ingestion]

  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }

  cleanup_on_fail = true
  timeout         = 600
  wait            = true
}

resource "null_resource" "wait_strimzi_crds" {
  depends_on = [helm_release.strimzi_operator]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-SCRIPT
      set -e
      echo "[CRD wait] Attente des CRDs Strimzi..."
      for crd in kafkas.kafka.strimzi.io kafkatopics.kafka.strimzi.io kafkausers.kafka.strimzi.io; do
        echo -n "  $crd ..."
        for i in $(seq 1 60); do
          kubectl get crd "$crd" &>/dev/null && break
          echo -n "."
          sleep 3
        done
        kubectl get crd "$crd" &>/dev/null || { echo " TIMEOUT"; exit 1; }
        kubectl wait crd "$crd" --for=condition=Established --timeout=60s
        echo " OK"
      done
      echo "[CRD wait] CRDs Strimzi prets."
    SCRIPT
  }
}
EOF

  #flink.tf
  cat > "$TERRAFORM_DIR/flink.tf" << 'EOF'
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

  timeout = 900
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

EOF

  # minio.tf
  cat > "$TERRAFORM_DIR/minio.tf" << 'EOF'
#  MinIO (standalone, mode POC)
resource "helm_release" "minio" {
  name             = "minio"
  repository       = "https://charts.min.io/"
  chart            = "minio"
  version          = var.minio_chart_version
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

  cleanup_on_fail = true
  timeout = 600
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
EOF

  # outputs.tf
  cat > "$TERRAFORM_DIR/outputs.tf" << 'EOF'
output "kafka_bootstrap" {
  value       = "${var.kafka_cluster_name}-kafka-bootstrap.${var.ingestion_namespace}.svc:9092"
  description = "Adresse interne du broker Kafka"
}

output "minio_endpoint" {
  value       = "http://minio.${var.stockage_namespace}.svc:9000"
  description = "Endpoint MinIO interne au cluster"
}

output "flink_rest_url" {
  value       = "http://poc-pipeline-rest.${var.traitement_namespace}.svc:8081"
  description = "API REST Flink pour soumettre les jobs"
}

output "minio_bucket" {
  value       = var.minio_bucket
  description = "Bucket MinIO pour les paiements enrichis"
}
EOF

  success "Fichiers Terraform générés dans $TERRAFORM_DIR"
}

# Génération des fichiers Terraform stage2 (ressources nécessitant les CRDs)
# Ce répertoire est appliqué APRÈS que les CRDs soient enregistrées.
# Isoler ces ressources ici empêche terraform plan/validate de les voir prématurément.
generate_stage2_files() {
  step "Génération des fichiers Terraform stage2 (ressources CRD)"
  local S2="$TERRAFORM_DIR/stage2"
  mkdir -p "$S2"

  # main.tf stage2 — réutilise le même cluster minikube
  cat > "$S2/main.tf" << 'EOF'
terraform {
  required_version = ">= 1.7"
  required_providers {
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.27" }
  }
}
provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "minikube"
}
EOF

  # variables.tf stage2
  cat > "$S2/variables.tf" << EOF
variable "ingestion_namespace"  { default = "$KAFKA_NAMESPACE" }
variable "traitement_namespace" { default = "$FLINK_NAMESPACE" }
variable "stockage_namespace"   { default = "$MINIO_NAMESPACE" }
variable "kafka_version"        { default = "$KAFKA_VERSION" }
variable "kafka_cluster_name"   { default = "$KAFKA_CLUSTER_NAME" }
variable "kafka_replicas"       { default = $KAFKA_REPLICAS }
variable "kafka_partitions"     { default = $KAFKA_PARTITIONS }
variable "minio_bucket"         { default = "$MINIO_BUCKET" }
variable "minio_access_key"     { default = "$MINIO_ACCESS_KEY" }
variable "minio_secret_key"     { default = "$MINIO_SECRET_KEY" }
variable "jobs_image"           { default = "$JOBS_IMAGE" }
variable "fernet_key" {
  description = "Fernet key shared by producer (encrypt) and Job 1 (decrypt)"
  sensitive   = true
}
EOF

  # kafka-crd.tf — Kafka cluster + topics
  cat > "$S2/kafka-crd.tf" << 'EOF'
resource "kubernetes_manifest" "kafka_cluster" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "Kafka"
    metadata = {
      name      = var.kafka_cluster_name
      namespace = var.ingestion_namespace
    }
    spec = {
      kafka = {
        version  = var.kafka_version
        replicas = var.kafka_replicas
        listeners = [
          { name = "plain",    port = 9092, type = "internal", tls = false },
          { name = "external", port = 9094, type = "nodeport", tls = false }
        ]
        config = {
          "offsets.topic.replication.factor"         = "1"
          "transaction.state.log.replication.factor" = "1"
          "transaction.state.log.min.isr"            = "1"
          "default.replication.factor"               = "1"
          "min.insync.replicas"                      = "1"

        }
        storage   = { type = "ephemeral" }
        resources = {
          requests = { memory = "512Mi", cpu = "250m" }
          limits   = { memory = "1Gi",  cpu = "500m" }
        }
      }
      zookeeper = {
        replicas  = 1
        storage   = { type = "ephemeral" }
        resources = {
          requests = { memory = "256Mi", cpu = "100m" }
          limits   = { memory = "512Mi", cpu = "200m" }
        }
      }
      entityOperator = { topicOperator = {}, userOperator = {} }
    }
  }
}

# Pipeline topics: ingestion → 4 stages → DLQ
locals {
  pipeline_topics = {
    "payments"            = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.decrypted"  = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.validated"  = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.normalized" = { partitions = var.kafka_partitions, retention_ms = 604800000 }
    "payments.dlq"        = { partitions = 1,                    retention_ms = 2592000000 }
  }
}

resource "kubernetes_manifest" "pipeline_topics" {
  for_each = local.pipeline_topics
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      # Resource name must match RFC1123 (no dots) — Strimzi reads spec.topicName for the actual Kafka name.
      name      = replace(each.key, ".", "-")
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      topicName  = each.key
      partitions = each.value.partitions
      replicas   = 1
      config = {
        "retention.ms"     = tostring(each.value.retention_ms)
        "cleanup.policy"   = "delete"
        "compression.type" = "lz4"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}
EOF

  # flink-crd.tf — Fernet secrets + 4 application-mode FlinkDeployments
  # local:// jarURI only works in application mode (JM reads its own fs).
  # FlinkSessionJob requires the operator to upload the jar, which fails for local://.
  # Memory tuning: reduce jvm-metaspace (256m→128m) and jvm-overhead.min (192m→64m)
  # so 768m JM and 640m TM both pass Flink's memory validation.
  cat > "$S2/flink-crd.tf" << 'EOF'
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
      # JM 640m: metaspace(128)+overhead(64)=192 → Flink memory=448m ✓
      # TM 640m: metaspace(128)+overhead(64)=192 → Flink memory=448m ✓
      jobManager  = { resource = { memory = "640m", cpu = 0.25 }, replicas = 1 }
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
EOF

  success "Fichiers Terraform stage2 générés dans $S2"
}

#  Génération des manifests K8s complémentaires
generate_k8s_manifests() {
  step "Génération des manifests Kubernetes complémentaires"
  mkdir -p "$K8S_MANIFESTS_DIR"

  # ServiceAccount Flink
  cat > "$K8S_MANIFESTS_DIR/flink-rbac.yaml" << EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink
  namespace: $FLINK_NAMESPACE
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: flink-role-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind:      ClusterRole
  name:      edit
subjects:
  - kind:      ServiceAccount
    name:      flink
    namespace: $FLINK_NAMESPACE
EOF

  # NetworkPolicy : Flink → Kafka
  cat > "$K8S_MANIFESTS_DIR/network-policies.yaml" << EOF
# Traitement peut accéder à Ingestion (Kafka)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-traitement-to-ingestion
  namespace: $KAFKA_NAMESPACE
spec:
  podSelector:
    matchLabels:
      strimzi.io/cluster: $KAFKA_CLUSTER_NAME
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: $FLINK_NAMESPACE
      ports:
        - port: 9092
---
# Traitement peut accéder à Stockage (MinIO)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-traitement-to-stockage
  namespace: $MINIO_NAMESPACE
spec:
  podSelector:
    matchLabels:
      app: minio
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: $FLINK_NAMESPACE
      ports:
        - port: 9000
EOF

  success "Manifests K8s générés dans $K8S_MANIFESTS_DIR"
}

# Note: producer.py is now a tracked source file in producer/.
# Same for the 4 PyFlink jobs in flink-jobs/.

# Nettoyage des résidus Helm/RBAC avant apply (évite les conflits lors d'un re-déploiement)
cleanup_before_deploy() {
  step "Nettoyage pré-déploiement (résidus Helm, RBAC, CRDs)"

  # ── Strimzi ──────────────────────────────────────────────────────────────
  # Désinstalle si présent, qu'il soit en état "deployed" ou "failed"
  local strimzi_status
  strimzi_status=$(helm status strimzi-kafka-operator -n "$KAFKA_NAMESPACE" \
                     --output json 2>/dev/null | grep -o '"status":"[^"]*"' | head -1 || true)
  if [[ -n "$strimzi_status" ]]; then
    warn "Release Helm 'strimzi-kafka-operator' trouvée (${strimzi_status}) → désinstallation forcée"
    helm uninstall strimzi-kafka-operator -n "$KAFKA_NAMESPACE" \
      --wait --timeout 120s --ignore-not-found 2>/dev/null || \
    helm uninstall strimzi-kafka-operator -n "$KAFKA_NAMESPACE" \
      --no-hooks --ignore-not-found 2>/dev/null || true
  fi

  # RoleBindings orphelins Strimzi — double approche :
  # 1. Suppression directe dans tous les namespaces connus
  # 2. Scan --all-namespaces pour attraper les namespaces inattendus
  log "Suppression des ClusterRole/ClusterRoleBinding Strimzi..."
  kubectl get clusterrole 2>/dev/null     | grep strimzi | awk '{print $1}'     | xargs -r kubectl delete clusterrole --ignore-not-found 2>/dev/null || true
  kubectl get clusterrolebinding 2>/dev/null     | grep strimzi | awk '{print $1}'     | xargs -r kubectl delete clusterrolebinding --ignore-not-found 2>/dev/null || true

  log "Suppression des RoleBindings Strimzi dans tous les namespaces..."
  local all_ns
  all_ns=$(kubectl get namespaces --no-headers -o custom-columns="NS:.metadata.name" 2>/dev/null || echo "")
  for rb in strimzi-cluster-operator strimzi-cluster-operator-watched             strimzi-cluster-operator-entity-operator-delegation             strimzi-cluster-operator-leader-election; do
    for ns in $all_ns; do
      [ -z "$ns" ] && continue
      kubectl delete rolebinding "$rb" -n "$ns" --ignore-not-found 2>/dev/null || true
    done
  done

    # ── Flink operator
  if helm status flink-kubernetes-operator -n "$FLINK_NAMESPACE" &>/dev/null; then
    warn "Release Helm 'flink-kubernetes-operator' déjà présente → désinstallation"
    helm uninstall flink-kubernetes-operator -n "$FLINK_NAMESPACE" --wait --timeout 120s || true
  fi

  # ── MinIO ────────────────────────────────────────────────────────────────
  if helm status minio -n "$MINIO_NAMESPACE" &>/dev/null; then
    warn "Release Helm 'minio' déjà présente → désinstallation"
    helm uninstall minio -n "$MINIO_NAMESPACE" --wait --timeout 120s || true
  fi


  # Le tfstate N'est JAMAIS supprimé ici — uniquement par teardown().
  # _tf_import_if_missing() gère la réconciliation state/cluster avant apply.

  # ── ConfigMap et Secrets orphelins ───────────────────────────────────────
  # Ces ressources légères sont supprimées si elles existent sans être dans
  # le tfstate — Terraform les recréera en quelques secondes.
  # (Évite "already exists" sans avoir à faire terraform import)
  log "Nettoyage ConfigMap et Secrets orphelins..."
  kubectl delete configmap flink-pipeline-config -n "$FLINK_NAMESPACE" --ignore-not-found 2>/dev/null || true
  kubectl delete secret minio-credentials -n "$FLINK_NAMESPACE" --ignore-not-found 2>/dev/null || true
  kubectl delete secret minio-credentials -n "$MINIO_NAMESPACE" --ignore-not-found 2>/dev/null || true

  # Supprimer tfstate si namespaces absents (désync après crash/arrêt brutal)
  if ! kubectl get namespace "$KAFKA_NAMESPACE" &>/dev/null && \
     [[ -f "$TERRAFORM_DIR/terraform.tfstate" ]]; then
    warn "Namespaces absents mais tfstate présent → suppression tfstate"
    rm -f "$TERRAFORM_DIR/terraform.tfstate" "$TERRAFORM_DIR/terraform.tfstate.bak"
    rm -f "$TERRAFORM_DIR/stage2/terraform.tfstate" "$TERRAFORM_DIR/stage2/terraform.tfstate.bak"
  fi

  success "Nettoyage terminé — environnement prêt pour un déploiement propre"
}

# Application Terraform
# Importe dans le tfstate les ressources qui existent dans le cluster
# mais sont absentes du state — évite "already exists" après un state perdu.
_tf_import_if_missing() {
  local tfdir="$1"
  cd "$tfdir"

  # Helper: import if resource missing from state
  _import_if() {
    local res="$1" id="$2"
    if ! terraform state list 2>/dev/null | grep -qF "$res"; then
      warn "Import $res ($id)"
      terraform import -input=false "$res" "$id" 2>/dev/null || true
    fi
  }

  # Namespaces
  kubectl get namespace "$KAFKA_NAMESPACE" &>/dev/null && _import_if "kubernetes_namespace.ingestion"  "$KAFKA_NAMESPACE"
  kubectl get namespace "$FLINK_NAMESPACE" &>/dev/null && _import_if "kubernetes_namespace.traitement" "$FLINK_NAMESPACE"
  kubectl get namespace "$MINIO_NAMESPACE" &>/dev/null && _import_if "kubernetes_namespace.stockage"   "$MINIO_NAMESPACE"

  # Helm releases
  helm status strimzi-kafka-operator    -n "$KAFKA_NAMESPACE" &>/dev/null && _import_if "helm_release.strimzi_operator" "$KAFKA_NAMESPACE/strimzi-kafka-operator"
  helm status flink-kubernetes-operator -n "$FLINK_NAMESPACE" &>/dev/null && _import_if "helm_release.flink_operator"   "$FLINK_NAMESPACE/flink-kubernetes-operator"
  helm status minio                     -n "$MINIO_NAMESPACE" &>/dev/null && _import_if "helm_release.minio"             "$MINIO_NAMESPACE/minio"

  # ConfigMap flink-pipeline-config
  kubectl get configmap flink-pipeline-config -n "$FLINK_NAMESPACE" &>/dev/null &&     _import_if "kubernetes_config_map.flink_config" "$FLINK_NAMESPACE/flink-pipeline-config"

  # Secrets minio-credentials
  kubectl get secret minio-credentials -n "$MINIO_NAMESPACE" &>/dev/null &&     _import_if "kubernetes_secret.minio_creds"       "$MINIO_NAMESPACE/minio-credentials"
  kubectl get secret minio-credentials -n "$FLINK_NAMESPACE" &>/dev/null &&     _import_if "kubernetes_secret.minio_creds_flink" "$FLINK_NAMESPACE/minio-credentials"

  cd - > /dev/null
}

# Fernet key — generated once, persisted at FERNET_KEY_FILE, shared by producer + Job 1
ensure_fernet_key() {
  if [[ -s "$FERNET_KEY_FILE" ]]; then
    return 0
  fi
  log "Génération de la clé Fernet (sauvegardée dans $FERNET_KEY_FILE)"
  python3 - <<'PY' > "$FERNET_KEY_FILE"
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode(), end="")
PY
  chmod 600 "$FERNET_KEY_FILE"
  success "Clé Fernet générée"
}

apply_terraform_stage1() {
  step "Terraform stage 1 : namespaces, opérateurs, attente CRDs"
  cd "$TERRAFORM_DIR"
  terraform init -upgrade
  _tf_import_if_missing "$TERRAFORM_DIR"
  terraform apply -auto-approve
  cd - > /dev/null
  success "Stage 1 appliqué"
}

_tf_import_if_missing_stage2() {
  local S2="$TERRAFORM_DIR/stage2"
  cd "$S2"

  _imp() {
    local res="$1" id="$2"
    if ! terraform state list 2>/dev/null | grep -qF "$res"; then
      warn "Import stage2 : $res"
      terraform import -input=false "$res" "$id" 2>/dev/null || true
    fi
  }

  # Fernet secrets
  kubectl get secret fernet-key -n "$FLINK_NAMESPACE" &>/dev/null && \
    _imp "kubernetes_secret.fernet_traitement" "$FLINK_NAMESPACE/fernet-key"
  kubectl get secret fernet-key -n "$KAFKA_NAMESPACE" &>/dev/null && \
    _imp "kubernetes_secret.fernet_ingestion"  "$KAFKA_NAMESPACE/fernet-key"

  # Kafka cluster
  kubectl get kafka "$KAFKA_CLUSTER_NAME" -n "$KAFKA_NAMESPACE" &>/dev/null && \
    _imp "kubernetes_manifest.kafka_cluster" \
         "apiVersion=kafka.strimzi.io/v1beta2,kind=Kafka,namespace=$KAFKA_NAMESPACE,name=$KAFKA_CLUSTER_NAME"

  # KafkaTopics (resource name uses - not .)
  for topic in payments payments.decrypted payments.validated payments.normalized payments.dlq; do
    local rname="${topic//./-}"
    kubectl get kafkatopic "$rname" -n "$KAFKA_NAMESPACE" &>/dev/null && \
      _imp "kubernetes_manifest.pipeline_topics[\"$topic\"]" \
           "apiVersion=kafka.strimzi.io/v1beta2,kind=KafkaTopic,namespace=$KAFKA_NAMESPACE,name=$rname"
  done

  # FlinkDeployments
  for job in job1-decryption job2-validation job3-normalization job4-sink; do
    kubectl get flinkdeployment "$job" -n "$FLINK_NAMESPACE" &>/dev/null && \
      _imp "kubernetes_manifest.flink_jobs[\"$job\"]" \
           "apiVersion=flink.apache.org/v1beta1,kind=FlinkDeployment,namespace=$FLINK_NAMESPACE,name=$job"
  done

  cd - > /dev/null
}

apply_terraform_stage2() {
  step "Terraform stage 2 : Kafka cluster + topics + Fernet + FlinkDeployments"
  ensure_fernet_key
  export TF_VAR_fernet_key="$(cat "$FERNET_KEY_FILE")"
  local S2="$TERRAFORM_DIR/stage2"
  cd "$S2"
  terraform init -upgrade
  _tf_import_if_missing_stage2
  terraform apply -auto-approve
  cd - > /dev/null
  unset TF_VAR_fernet_key
  success "Stage 2 appliqué"
}

# ── Build the PyFlink jobs image directly inside minikube's Docker daemon ─
build_jobs_image() {
  step "Build de l'image PyFlink (${JOBS_IMAGE}) dans minikube"
  if [[ ! -f "$JOBS_DIR/Dockerfile" ]]; then
    error "Dockerfile introuvable dans $JOBS_DIR"
  fi
  # Switch docker CLI to talk to minikube's docker daemon, then build.
  # This avoids `docker save | minikube image load` round-trip and works with the docker driver.
  log "eval \$(minikube docker-env) && docker build ..."
  eval "$(minikube -p minikube docker-env --shell bash)"
  docker build -t "$JOBS_IMAGE" "$JOBS_DIR"
  success "Image $JOBS_IMAGE disponible dans le docker daemon de minikube"
}

# ── Wait for the 4 FlinkDeployments to reach STABLE ─────────────────────
wait_for_jobs() {
  step "Attente du démarrage des 4 jobs Flink"
  for job in job1-decryption job2-validation job3-normalization job4-sink; do
    log "  $job ..."
    local elapsed=0
    while true; do
      local phase
      phase=$(kubectl -n "$FLINK_NAMESPACE" get flinkdeployment "$job" \
                -o jsonpath='{.status.lifecycleState}' 2>/dev/null || echo "")
      [[ "$phase" == "STABLE" ]] && { success "  $job : STABLE"; break; }
      if (( elapsed >= 300 )); then
        warn "  $job : non-stable après ${elapsed}s (état=$phase) — vérifier kubectl logs"
        break
      fi
      sleep 5; (( elapsed += 5 ))
    done
  done
}

# ── Run the local producer against the minikube nodeport ────────────────
produce_csv() {
  local csv="${1:-}"
  [[ -z "$csv" ]] && error "Usage : $0 produce <chemin/vers/data.csv> [rate] [limit]"
  [[ ! -f "$csv" ]] && error "Fichier introuvable : $csv"
  local rate="${2:-20}"
  local limit="${3:-0}"

  ensure_fernet_key
  local mip kport bootstrap
  mip=$(minikube ip)
  kport=$(kubectl -n "$KAFKA_NAMESPACE" get svc "${KAFKA_CLUSTER_NAME}-kafka-external-bootstrap" \
           -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null || echo "")
  [[ -z "$kport" ]] && error "NodePort Kafka introuvable — l'infra est-elle prête ?"
  bootstrap="${mip}:${kport}"

  log "Producer → broker $bootstrap, topic payments, rate=${rate}/s limit=${limit}"
  PRODUCER_VENV="$(pwd)/.producer-venv"
  if [[ ! -d "$PRODUCER_VENV" ]]; then
    log "Création du venv producteur..."
    if ! python3 -m venv "$PRODUCER_VENV" 2>/dev/null; then
      log "python3-venv absent — installation via apt..."
      sudo apt-get install -y python3-venv -qq
      python3 -m venv "$PRODUCER_VENV"
    fi
  fi
  if ! "$PRODUCER_VENV/bin/python" -c "import kafka, pandas, cryptography" 2>/dev/null; then
    log "Installation des dépendances Python (kafka-python, pandas, cryptography)..."
    "$PRODUCER_VENV/bin/pip" install --quiet -r "$PRODUCER_DIR/requirements.txt"
  fi

  FERNET_KEY="$(cat "$FERNET_KEY_FILE")" \
    PYTHONPATH="$JOBS_DIR" \
    "$PRODUCER_VENV/bin/python" "$PRODUCER_DIR/producer.py" \
      --csv "$csv" \
      --bootstrap "$bootstrap" \
      --rate "$rate" \
      --limit "$limit"
}

#  Application RBAC & network policies
apply_k8s_extras() {
  step "Application des manifests Kubernetes complémentaires"
  kubectl apply -f "$K8S_MANIFESTS_DIR/flink-rbac.yaml"
  kubectl apply -f "$K8S_MANIFESTS_DIR/network-policies.yaml"
  success "RBAC et NetworkPolicies appliqués"
}

# Attente de la disponibilité des composants
wait_for_components() {
  step "Attente de la disponibilité des composants"

  log "Attente du pod Strimzi operator (Running)…"
  kubectl rollout status deployment/strimzi-cluster-operator \
    -n "$KAFKA_NAMESPACE" \
    --timeout=120s

  log "Attente de Kafka (KafkaCluster ready)…"
  kubectl wait kafka/"$KAFKA_CLUSTER_NAME" \
    -n "$KAFKA_NAMESPACE" \
    --for=condition=Ready \
    --timeout=600s

  log "Attente de MinIO…"
  kubectl rollout status deployment/minio \
    -n "$MINIO_NAMESPACE" \
    --timeout=300s

  success "Kafka + MinIO opérationnels"
}

#Affichage du résumé
print_summary() {
  step "Résumé de l'infrastructure POC"

  MINIKUBE_IP=$(minikube ip 2>/dev/null || echo "<minikube-ip>")

  echo -e """
${GREEN}╔══════════════════════════════════════════════════════════╗
║       RT-Payments Pipeline — Infrastructure prête        ║
╚══════════════════════════════════════════════════════════╝${NC}

${CYAN}Pipeline :${NC}
  producer.py
      ↓ payments              (envelope chiffré Fernet)
  Job 1 — Décryption          flinkdeployment/job1-decryption
      ↓ payments.decrypted
  Job 2 — Validation          flinkdeployment/job2-validation
      ↓ payments.validated         ↘ payments.dlq  (rejets ISO)
  Job 3 — Normalisation       flinkdeployment/job3-normalization
      ↓ payments.normalized   (canonique, ISO 8601 / PCI mask / MCC enrichi)
  Job 4 — Sink                flinkdeployment/job4-sink
      ↓ s3a://${MINIO_BUCKET}/canonical/businessDate=YYYY-MM-DD/mti=XXXX/

${CYAN}Statut des jobs :${NC}
  kubectl get flinkdeployment -n $FLINK_NAMESPACE
  kubectl get pods -n $FLINK_NAMESPACE
  kubectl logs -n $FLINK_NAMESPACE -l app=job1-decryption -c flink-main-container --tail=50

${CYAN}Flink UI (par job) :${NC}
  kubectl port-forward -n $FLINK_NAMESPACE svc/job1-decryption-rest 8081:8081
  # → http://localhost:8081

${CYAN}MinIO :${NC}
  kubectl port-forward -n $MINIO_NAMESPACE svc/minio-console 9001:9001
  # → http://localhost:9001  ($MINIO_ACCESS_KEY / $MINIO_SECRET_KEY)
  Bucket : $MINIO_BUCKET

${CYAN}Lancer le producer :${NC}
  ./setup_poc.sh produce <chemin/vers/csv> [rate=20] [limit=0]

${CYAN}Inspecter les topics :${NC}
  kubectl exec -n $KAFKA_NAMESPACE -it ${KAFKA_CLUSTER_NAME}-kafka-0 -- \\
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \\
    --topic payments.normalized --from-beginning --max-messages 5

${YELLOW}Arrêter :${NC}  ./setup_poc.sh down
"""
}

# Nettoyage
teardown() {
  step "Teardown de l'infrastructure POC"
  warn "Cette action va détruire TOUTE l'infrastructure."
  read -r -p "Confirmer la destruction ? (yes/no) : " confirm
  [[ "$confirm" == "yes" ]] || { log "Annulé."; exit 0; }

  # 0. Nettoyage CDC (PostgreSQL + Debezium + job0) si déployés
  log "Suppression des ressources CDC (si présentes)..."
  kubectl delete flinkdeployment job0-cdc-adapter -n "$FLINK_NAMESPACE" --ignore-not-found 2>/dev/null || true
  kubectl delete deployment postgresql debezium-connect -n "$PG_NAMESPACE" --ignore-not-found 2>/dev/null || true
  kubectl delete service postgresql debezium-connect -n "$PG_NAMESPACE" --ignore-not-found 2>/dev/null || true
  kubectl delete configmap postgresql-init -n "$PG_NAMESPACE" --ignore-not-found 2>/dev/null || true

  # 1. Désinstaller les Helm releases explicitement (Terraform peut échouer
  #    si des CRDs/RoleBindings sont déjà dans un état incohérent)
  log "Désinstallation des releases Helm..."
  helm uninstall strimzi-kafka-operator  -n "$KAFKA_NAMESPACE"  --ignore-not-found --wait --timeout 120s 2>/dev/null || true
  helm uninstall flink-kubernetes-operator -n "$FLINK_NAMESPACE" --ignore-not-found --wait --timeout 120s 2>/dev/null || true
  helm uninstall minio                   -n "$MINIO_NAMESPACE"  --ignore-not-found --wait --timeout 120s 2>/dev/null || true

  # 2. Supprimer les RoleBindings orphelins de Strimzi dans TOUS les namespaces
  log "Suppression des RoleBindings Strimzi dans tous les namespaces POC..."
  for ns in ingestion traitement stockage default; do
    for rb in strimzi-cluster-operator strimzi-cluster-operator-watched \
              strimzi-cluster-operator-entity-operator-delegation \
              strimzi-cluster-operator-leader-election; do
      kubectl delete rolebinding "$rb" -n "$ns" --ignore-not-found 2>/dev/null || true
    done
    kubectl get secrets -n "$ns" -o name 2>/dev/null \
      | grep strimzi | xargs -r kubectl delete -n "$ns" --ignore-not-found 2>/dev/null || true
  done

  # 3. Patch finalizers Kafka avant terraform destroy
  log "Patch finalizers Kafka/KafkaTopic..."
  kubectl get kafka -n ingestion --no-headers -o custom-columns="NAME:.metadata.name" 2>/dev/null     | xargs -r -I{} kubectl patch kafka {} -n ingestion         --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true
  kubectl get kafkatopic -n ingestion --no-headers -o custom-columns="NAME:.metadata.name" 2>/dev/null     | xargs -r -I{} kubectl patch kafkatopic {} -n ingestion         --type=merge -p '{"metadata":{"finalizers":[]}}' 2>/dev/null || true

  # 4. Terraform destroy (namespaces + secrets + configmaps restants)
  # Détruire stage2 d'abord (CRD resources), puis stage1 (operators/namespaces)
  # Force-clear finalizers en arriere-plan pendant terraform destroy
  _force_clear_bg() {
    while true; do
      for ns in ingestion traitement stockage; do
        kubectl get namespace "$ns" -o json 2>/dev/null           | jq '.spec.finalizers = []'           | kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
      done
      sleep 5
    done
  }
  _force_clear_bg &
  local _bg_pid=$!

  local S2="$TERRAFORM_DIR/stage2"
  if [[ -f "$S2/terraform.tfstate" ]]; then
    cd "$S2"
    terraform destroy -auto-approve 2>/dev/null || true
    cd - > /dev/null
  fi
  if [[ -f "$TERRAFORM_DIR/terraform.tfstate" ]]; then
    cd "$TERRAFORM_DIR"
    terraform destroy -auto-approve 2>/dev/null || true
    cd - > /dev/null
  fi
  kill $_bg_pid 2>/dev/null || true

  # 5. Supprimer les namespaces et ATTENDRE leur disparition complète
  #    (évite la race condition "namespace already exists / Terminating" au prochain 'up')
  for ns in "$KAFKA_NAMESPACE" "$FLINK_NAMESPACE" "$MINIO_NAMESPACE"; do
    kubectl delete namespace "$ns" --ignore-not-found 2>/dev/null || true
  done

  log "Attente de la terminaison complète des namespaces..."
  for ns in "$KAFKA_NAMESPACE" "$FLINK_NAMESPACE" "$MINIO_NAMESPACE"; do
    elapsed=0
    while kubectl get namespace "$ns" &>/dev/null; do
      if (( elapsed >= 90 )); then
        warn "Namespace '$ns' bloqué en Terminating depuis ${elapsed}s → suppression forcée des finalizers"
        kubectl get namespace "$ns" -o json \
          | jq '.spec.finalizers = []' \
          | kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
        sleep 5
        break
      fi
      log "  '$ns' encore en Terminating... (${elapsed}s)"
      sleep 5
      (( elapsed += 5 ))
    done
    success "Namespace '$ns' supprimé"
  done

  # 6. Nettoyer le tfstate pour que le prochain 'up' reparte de zéro
  rm -f "$TERRAFORM_DIR/terraform.tfstate" \
        "$TERRAFORM_DIR/terraform.tfstate.bak" \
        "$TERRAFORM_DIR/terraform.tfstate.lock.info" \
        "$TERRAFORM_DIR/tfplan"
  rm -f "$TERRAFORM_DIR/stage2/terraform.tfstate" \
        "$TERRAFORM_DIR/stage2/terraform.tfstate.bak" \
        "$TERRAFORM_DIR/stage2/terraform.tfstate.lock.info"

  # 7. Arrêter Minikube proprement (stop suffit — on garde le cluster pour éviter
  #    les cgroup issues au prochain démarrage ; delete --purge seulement si demandé)
  log "Arrêt de Minikube..."
  minikube stop 2>/dev/null || true
  success "Infrastructure détruite et Minikube arrêté — prêt pour un nouveau 'up'"
}

# ── Attend que l'API REST Debezium soit prête et enregistre le connecteur ──
register_debezium_connector() {
  log "Attente du pod Debezium Connect (max 120s)..."
  local pod elapsed=0
  until pod=$(kubectl get pod -n "$PG_NAMESPACE" -l app=debezium-connect \
              -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) && [[ -n "$pod" ]]; do
    sleep 4; (( elapsed += 4 ))
    (( elapsed >= 120 )) && error "Timeout : pod Debezium Connect introuvable"
  done

  log "Attente de l'API REST Debezium sur :8083 (max 180s)..."
  elapsed=0
  until kubectl exec -n "$PG_NAMESPACE" "$pod" -- \
        curl -sf http://localhost:8083/connectors &>/dev/null; do
    sleep 5; (( elapsed += 5 ))
    (( elapsed >= 180 )) && error "Timeout : API REST Debezium non disponible"
  done
  success "API REST Debezium disponible"

  # Create the replication publication as the postgres superuser BEFORE registering
  # the connector. The debezium user has REPLICATION LOGIN but not CREATE on the
  # database, so Debezium cannot auto-create it with publication.autocreate.mode=filtered.
  log "Création de la publication de réplication (en tant que superuser postgres)..."
  kubectl exec -n "$PG_NAMESPACE" deployment/postgresql -- \
    psql -U "$PG_USER" -d "$PG_DB" -c \
    "CREATE PUBLICATION dbz_publication FOR TABLE public.powercard_operations;" \
    2>/dev/null || warn "Publication déjà existante — OK"

  log "Enregistrement du connecteur powercard-connector..."
  local pg_host="postgresql.${PG_NAMESPACE}.svc.cluster.local"
  kubectl exec -n "$PG_NAMESPACE" "$pod" -- \
    curl -sf -X POST http://localhost:8083/connectors \
      -H 'Content-Type: application/json' \
      -d "{
        \"name\": \"powercard-connector\",
        \"config\": {
          \"connector.class\": \"io.debezium.connector.postgresql.PostgresConnector\",
          \"tasks.max\": \"1\",
          \"database.hostname\": \"${pg_host}\",
          \"database.port\": \"5432\",
          \"database.user\": \"${DEBEZIUM_USER}\",
          \"database.password\": \"${DEBEZIUM_PASSWORD}\",
          \"database.dbname\": \"${PG_DB}\",
          \"topic.prefix\": \"cdc\",
          \"table.include.list\": \"public.powercard_operations\",
          \"plugin.name\": \"pgoutput\",
          \"slot.name\": \"debezium_slot\",
          \"publication.name\": \"dbz_publication\",
          \"publication.autocreate.mode\": \"disabled\",
          \"decimal.handling.mode\": \"string\",
          \"time.precision.mode\": \"adaptive\"
        }
      }" 2>/dev/null && success "Connecteur enregistré" || \
    warn "Connecteur déjà existant ou erreur d'enregistrement — vérifier l'état manuellement"
}

# ── Déploie PostgreSQL + Debezium Connect + Job 0 CDC Adapter ─────────────
apply_cdc() {
  step "Déploiement CDC : PostgreSQL + Debezium Connect + Job 0"

  local bootstrap="${KAFKA_CLUSTER_NAME}-kafka-bootstrap.${KAFKA_NAMESPACE}.svc:9092"

  # ── 1. ConfigMap init SQL PostgreSQL ────────────────────────────────────
  log "Création du ConfigMap postgresql-init..."
  kubectl apply -n "$PG_NAMESPACE" -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgresql-init
  namespace: ${PG_NAMESPACE}
data:
  01-schema.sql: |
    CREATE TABLE IF NOT EXISTS public.powercard_operations (
        id                              BIGSERIAL PRIMARY KEY,
        message_type                    TEXT,
        function_code                   TEXT,
        processing_code                 TEXT,
        action_code                     TEXT,
        original_action_code            TEXT,
        issuer_action_code              TEXT,
        event_code                      TEXT,
        network_code                    TEXT,
        issuing_bank                    TEXT,
        transaction_local_date          TEXT,
        transmission_date_and_time      TEXT,
        response_date_and_time          TEXT,
        internal_transmission_time      TEXT,
        capture_date                    TEXT,
        business_date                   TEXT,
        product_code                    TEXT,
        card_type                       TEXT,
        transaction_amount              TEXT,
        cash_back_amount                TEXT,
        transaction_currency            TEXT,
        replacement_amount              TEXT,
        billing_amount                  TEXT,
        billing_currency                TEXT,
        conversion_rate                 TEXT,
        conversion_rate_date            TEXT,
        iss_settlement_amount           TEXT,
        iss_settlement_currency         TEXT,
        iss_settlement_date             TEXT,
        iss_conv_rate_settlement        TEXT,
        iss_conv_rate_settlement_date   TEXT,
        acq_settlement_amount           TEXT,
        acq_settlement_currency         TEXT,
        acq_settlement_date             TEXT,
        acq_conv_rate_settlement        TEXT,
        acq_conv_rate_settlement_date   TEXT,
        services_setup_code             TEXT,
        receiving_institution           TEXT,
        acquirer_institution_code       TEXT,
        acquirer_bank                   TEXT,
        card_acceptor_activity          TEXT,
        tcc                             TEXT,
        card_acceptor_term_id           TEXT,
        card_acceptor_id                TEXT,
        card_acc_name_address           TEXT,
        pos_entry_mode                  TEXT,
        pos_condition_code              TEXT,
        pos_data                        TEXT,
        forwarding_country_code         TEXT,
        forwarding_institution_code     TEXT,
        forwarding_bank                 TEXT,
        authorization_length            TEXT,
        authorization_code              TEXT,
        original_authorization_code     TEXT,
        authorization_source            TEXT,
        security_verif_level            TEXT,
        security_verif_result           TEXT,
        reject_code                     TEXT,
        reject_reason                   TEXT,
        reason_code                     TEXT,
        origine_code                    TEXT,
        original_transaction_date_time  TEXT,
        reversal_stan                   TEXT,
        reversal_transaction_date       TEXT,
        autho_flag                      TEXT,
        reversal_flag                   TEXT,
        transaction_flag                TEXT,
        matching_status                 TEXT,
        matching_date                   TEXT,
        matching_level                  TEXT,
        matching_date_purge             TEXT,
        private_tlv_data                TEXT,
        authorization_id                TEXT,
        transaction_id                  TEXT,
        user_create                     TEXT,
        date_create                     TEXT,
        user_modif                      TEXT,
        date_modif                      TEXT,
        internal_stan                   TEXT,
        external_stan                   TEXT,
        reference_number                TEXT,
        routing_code                    TEXT,
        capture_code                    TEXT,
        acquirer_resource_code          TEXT,
        acquiring_country_code          TEXT,
        card_number                     TEXT,
        card_sequence_number            TEXT,
        chip_cryptogram_info_data       TEXT,
        destination_account_code        TEXT,
        destination_account_entity_cod  TEXT,
        destination_account_entity_id   TEXT,
        destination_account_number      TEXT,
        destination_account_type        TEXT,
        end_expiry_date                 TEXT,
        external_cvv_result_code        TEXT,
        issuer_resource_code            TEXT,
        limit_id                        TEXT,
        limit_index                     TEXT,
        network_data                    TEXT,
        network_id                      TEXT,
        original_table_indicator        TEXT,
        service_code                    TEXT,
        source_account_code             TEXT,
        source_account_entity_code      TEXT,
        source_account_entity_id        TEXT,
        source_account_number           TEXT,
        source_account_type             TEXT,
        start_expiry_date               TEXT,
        current_table_indicator         TEXT,
        ingested_at                     TIMESTAMPTZ DEFAULT NOW()
    );
    ALTER TABLE public.powercard_operations REPLICA IDENTITY FULL;
  02-debezium-user.sql: |
    DO \$\$ BEGIN
      CREATE USER ${DEBEZIUM_USER} WITH REPLICATION LOGIN PASSWORD '${DEBEZIUM_PASSWORD}';
    EXCEPTION WHEN duplicate_object THEN RAISE NOTICE 'User ${DEBEZIUM_USER} already exists'; END \$\$;
    GRANT CONNECT ON DATABASE ${PG_DB} TO ${DEBEZIUM_USER};
    GRANT USAGE ON SCHEMA public TO ${DEBEZIUM_USER};
    GRANT SELECT ON public.powercard_operations TO ${DEBEZIUM_USER};
EOF

  # ── 2. PostgreSQL Deployment + Service ──────────────────────────────────
  log "Déploiement PostgreSQL..."
  kubectl apply -n "$PG_NAMESPACE" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgresql
  namespace: ${PG_NAMESPACE}
  labels:
    app: postgresql
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgresql
  template:
    metadata:
      labels:
        app: postgresql
    spec:
      containers:
      - name: postgresql
        image: postgres:15
        args: ["-c", "wal_level=logical", "-c", "max_replication_slots=4",
               "-c", "max_wal_senders=4"]
        env:
        - name: POSTGRES_DB
          value: "${PG_DB}"
        - name: POSTGRES_USER
          value: "${PG_USER}"
        - name: POSTGRES_PASSWORD
          value: "${PG_PASSWORD}"
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: init-sql
          mountPath: /docker-entrypoint-initdb.d
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "300m"
      volumes:
      - name: init-sql
        configMap:
          name: postgresql-init
---
apiVersion: v1
kind: Service
metadata:
  name: postgresql
  namespace: ${PG_NAMESPACE}
spec:
  selector:
    app: postgresql
  ports:
  - port: 5432
    targetPort: 5432
EOF

  # ── 3. Debezium Connect Deployment + Service ────────────────────────────
  log "Déploiement Debezium Connect (${DEBEZIUM_CONNECT_IMAGE})..."
  kubectl apply -n "$PG_NAMESPACE" -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: debezium-connect
  namespace: ${PG_NAMESPACE}
  labels:
    app: debezium-connect
spec:
  replicas: 1
  selector:
    matchLabels:
      app: debezium-connect
  template:
    metadata:
      labels:
        app: debezium-connect
    spec:
      containers:
      - name: debezium-connect
        image: ${DEBEZIUM_CONNECT_IMAGE}
        env:
        - name: BOOTSTRAP_SERVERS
          value: "${bootstrap}"
        - name: GROUP_ID
          value: "debezium-connect-1"
        - name: CONFIG_STORAGE_TOPIC
          value: "debezium_connect_configs"
        - name: OFFSET_STORAGE_TOPIC
          value: "debezium_connect_offsets"
        - name: STATUS_STORAGE_TOPIC
          value: "debezium_connect_statuses"
        - name: CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE
          value: "false"
        - name: CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE
          value: "false"
        ports:
        - containerPort: 8083
        resources:
          requests:
            memory: "256Mi"
            cpu: "200m"
          limits:
            memory: "512Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: debezium-connect
  namespace: ${PG_NAMESPACE}
spec:
  selector:
    app: debezium-connect
  ports:
  - port: 8083
    targetPort: 8083
EOF

  # ── 4. Attente PostgreSQL + Debezium ────────────────────────────────────
  log "Attente PostgreSQL (rollout)..."
  kubectl rollout status deployment/postgresql -n "$PG_NAMESPACE" --timeout=120s

  log "Attente Debezium Connect (rollout)..."
  kubectl rollout status deployment/debezium-connect -n "$PG_NAMESPACE" --timeout=300s

  # ── 5. Enregistrement du connecteur ─────────────────────────────────────
  register_debezium_connector

  # ── 6. Rebuild image (job0 ajouté) + FlinkDeployment job0 ───────────────
  build_jobs_image

  log "Déploiement FlinkDeployment job0-cdc-adapter..."
  kubectl apply -n "$FLINK_NAMESPACE" -f - <<EOF
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: job0-cdc-adapter
  namespace: ${FLINK_NAMESPACE}
  labels:
    app.kubernetes.io/part-of: rt-payments
    app.kubernetes.io/component: job0-cdc-adapter
spec:
  image: ${JOBS_IMAGE}
  imagePullPolicy: Never
  flinkVersion: v1_18
  serviceAccount: flink
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "1"
    state.backend: hashmap
    state.checkpoints.dir: "file:///tmp/flink-checkpoints"
    jobmanager.memory.jvm-metaspace.size: "128mb"
    jobmanager.memory.jvm-overhead.min: "64mb"
    taskmanager.memory.jvm-metaspace.size: "128mb"
    taskmanager.memory.jvm-overhead.min: "64mb"
    taskmanager.memory.managed.fraction: "0.2"
    s3.endpoint: "http://minio.${MINIO_NAMESPACE}.svc:9000"
    s3.path.style.access: "true"
    s3.access-key: "${MINIO_ACCESS_KEY}"
    s3.secret-key: "${MINIO_SECRET_KEY}"
  jobManager:
    resource:
      memory: "640m"
      cpu: 0.25
    replicas: 1
  taskManager:
    resource:
      memory: "640m"
      cpu: 0.25
    replicas: 1
  podTemplate:
    spec:
      containers:
      - name: flink-main-container
        env:
        - name: KAFKA_BOOTSTRAP
          value: "${bootstrap}"
        - name: S3_BUCKET
          value: "${MINIO_BUCKET}"
  job:
    jarURI: "local:///opt/flink/python-driver/flink-python.jar"
    entryClass: "org.apache.flink.client.python.PythonDriver"
    args: ["-pyclientexec", "/usr/bin/python3", "-py", "/opt/jobs/job0_cdc_adapter.py"]
    parallelism: 1
    upgradeMode: stateless
EOF

  success "CDC déployé ✓"
  echo -e """
${CYAN}Insérer une ligne de test dans PostgreSQL :${NC}
  kubectl exec -n ${PG_NAMESPACE} deployment/postgresql -- \\
    psql -U ${PG_USER} -d ${PG_DB} -c \\
    \"INSERT INTO powercard_operations (message_type, transaction_currency, transaction_amount, card_number) \\
      VALUES ('1100', '504', '100', '5321962100025057')\"

${CYAN}Vérifier le statut du connecteur Debezium :${NC}
  kubectl port-forward svc/debezium-connect -n ${PG_NAMESPACE} 8083:8083 &
  curl -s http://localhost:8083/connectors/powercard-connector/status | python3 -m json.tool

${CYAN}Flink UI Job 0 :${NC}
  kubectl port-forward svc/job0-cdc-adapter-rest -n ${FLINK_NAMESPACE} 8080:8081
  # → http://localhost:8080
"""
}

# ── Load a Powercard CSV directly into PostgreSQL (triggers CDC → pipeline) ──
load_csv() {
  local csv="${1:-}"
  [[ -z "$csv" ]] && error "Usage : $0 load-csv <chemin/vers/data.csv>"
  [[ ! -f "$csv" ]] && error "Fichier introuvable : $csv"

  step "Chargement CSV → PostgreSQL (CDC)"

  # Find the PostgreSQL pod
  local pg_pod
  pg_pod=$(kubectl get pod -n "$PG_NAMESPACE" -l app=postgresql \
           -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -z "$pg_pod" ]] && error "Pod PostgreSQL introuvable — lancer './setup_poc.sh cdc' d'abord"

  log "Pod PostgreSQL : $pg_pod"
  log "Copie du fichier CSV dans le pod..."
  kubectl cp "$csv" "$PG_NAMESPACE/$pg_pod:/tmp/powercard_data.csv"

  log "Chargement via COPY (bulk insert → Debezium capturera les lignes)..."
  kubectl exec -n "$PG_NAMESPACE" "$pg_pod" -- \
    psql -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -c "
COPY public.powercard_operations (
  message_type, function_code, processing_code, action_code, original_action_code,
  issuer_action_code, event_code, network_code, issuing_bank, transaction_local_date,
  transmission_date_and_time, response_date_and_time, internal_transmission_time,
  capture_date, business_date, product_code, card_type, transaction_amount,
  cash_back_amount, transaction_currency, replacement_amount, billing_amount,
  billing_currency, conversion_rate, conversion_rate_date, iss_settlement_amount,
  iss_settlement_currency, iss_settlement_date, iss_conv_rate_settlement,
  iss_conv_rate_settlement_date, acq_settlement_amount, acq_settlement_currency,
  acq_settlement_date, acq_conv_rate_settlement, acq_conv_rate_settlement_date,
  services_setup_code, receiving_institution, acquirer_institution_code,
  acquirer_bank, card_acceptor_activity, tcc, card_acceptor_term_id,
  card_acceptor_id, card_acc_name_address, pos_entry_mode, pos_condition_code,
  pos_data, forwarding_country_code, forwarding_institution_code, forwarding_bank,
  authorization_length, authorization_code, original_authorization_code,
  authorization_source, security_verif_level, security_verif_result, reject_code,
  reject_reason, reason_code, origine_code, original_transaction_date_time,
  reversal_stan, reversal_transaction_date, autho_flag, reversal_flag,
  transaction_flag, matching_status, matching_date, matching_level,
  matching_date_purge, private_tlv_data, authorization_id, transaction_id,
  user_create, date_create, user_modif, date_modif, internal_stan, external_stan,
  reference_number, routing_code, capture_code, acquirer_resource_code,
  acquiring_country_code, card_number, card_sequence_number,
  chip_cryptogram_info_data, destination_account_code,
  destination_account_entity_cod, destination_account_entity_id,
  destination_account_number, destination_account_type, end_expiry_date,
  external_cvv_result_code, issuer_resource_code, limit_id, limit_index,
  network_data, network_id, original_table_indicator, service_code,
  source_account_code, source_account_entity_code, source_account_entity_id,
  source_account_number, source_account_type, start_expiry_date,
  current_table_indicator
)
FROM '/tmp/powercard_data.csv'
WITH (FORMAT CSV, HEADER true, NULL '');
"

  local row_count
  row_count=$(kubectl exec -n "$PG_NAMESPACE" "$pg_pod" -- \
    psql -U "$PG_USER" -d "$PG_DB" -tAc "SELECT COUNT(*) FROM public.powercard_operations;")
  success "Import terminé — ${row_count} lignes dans powercard_operations"
  echo -e """
${CYAN}Suivi du pipeline CDC :${NC}
  # Consommer le topic Debezium (messages bruts CDC) :
  kubectl exec -n ${KAFKA_NAMESPACE} -it ${KAFKA_CLUSTER_NAME}-kafka-0 -- \\
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \\
    --topic cdc.public.powercard_operations --from-beginning --max-messages 3

  # Vérifier payments.decrypted (sortie job0) :
  kubectl exec -n ${KAFKA_NAMESPACE} -it ${KAFKA_CLUSTER_NAME}-kafka-0 -- \\
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \\
    --topic payments.decrypted --from-beginning --max-messages 3

  # Vérifier payments.normalized (sortie job3) :
  kubectl exec -n ${KAFKA_NAMESPACE} -it ${KAFKA_CLUSTER_NAME}-kafka-0 -- \\
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \\
    --topic payments.normalized --from-beginning --max-messages 3

  # Vérifier les fichiers Parquet dans MinIO :
  kubectl port-forward -n ${MINIO_NAMESPACE} svc/minio-console 9001:9001
  # → http://localhost:9001  ($MINIO_ACCESS_KEY / $MINIO_SECRET_KEY)
  # Bucket : ${MINIO_BUCKET}/canonical/
"""
}

#  Point d'entrée
usage() {
  echo -e """
Usage : $0 [COMMANDE]

  up                    Infrastructure complète (Minikube + opérateurs + jobs)
  build                 Construire l'image PyFlink dans le docker daemon de minikube
  jobs                  Réappliquer uniquement les 4 FlinkDeployments (stage2)
  cdc                   (Re)déployer PostgreSQL + Debezium + Job 0 (CDC pipeline)
                        Inclus automatiquement dans 'up' — utile pour redéployer seul
  load-csv <csv>        Charger un CSV Powercard dans PostgreSQL
                        Debezium capturera les INSERT → pipeline CDC complet
                        Prérequis : ./setup_poc.sh cdc déjà exécuté
  produce <csv> [rate] [limit]
                        Lancer le producer en local vers le NodePort Kafka
  down                  Détruire toute l'infrastructure (inclut les ressources CDC)
  plan                  Plan Terraform (partiel — sans CRDs)
  status                Statut des pods dans les 3 namespaces
  help                  Afficher cette aide

Variables :
  JOBS_IMAGE  (défaut: $JOBS_IMAGE)
  FERNET_KEY  (auto-généré dans .fernet.key)
"""
}

main() {
  case "${1:-up}" in
    up)
      check_prerequisites
      start_minikube
      generate_terraform_files
      generate_stage2_files
      generate_k8s_manifests
      cleanup_before_deploy
      apply_terraform_stage1     # operators + namespaces + CRD wait
      apply_k8s_extras           # flink SA + NetworkPolicies (must exist before FlinkDeployments)
      build_jobs_image           # image must exist before stage2 (imagePullPolicy=Never)
      apply_terraform_stage2     # Kafka cluster + topics + Fernet + 4 FlinkDeployments
      wait_for_components        # Kafka cluster Ready + MinIO rollout
      wait_for_jobs              # 4 FlinkDeployments STABLE
      apply_cdc                  # PostgreSQL + Debezium + job0 CDC adapter
      print_summary
      ;;
    build)
      build_jobs_image
      ;;
    jobs)
      apply_terraform_stage2
      wait_for_jobs
      ;;
    produce)
      shift
      produce_csv "$@"
      ;;
    down)
      teardown
      ;;
    plan)
      generate_terraform_files
      cd "$TERRAFORM_DIR"
      terraform init -upgrade -reconfigure
      # Plan partiel : les manifests CRD (Kafka, FlinkDeployment) ne peuvent pas
      # être planifiés tant que les CRDs ne sont pas installées dans le cluster.
      warn "Plan partiel (namespaces + operators) — les manifests CRD nécessitent un 'up' complet"
      terraform plan \
        -target=kubernetes_namespace.ingestion \
        -target=kubernetes_namespace.traitement \
        -target=kubernetes_namespace.stockage \
        -target=helm_release.strimzi_operator \
        -target=helm_release.flink_operator \
        -target=helm_release.minio
      ;;
    cdc)
      apply_cdc
      ;;
    load-csv)
      shift
      load_csv "$@"
      ;;
    status)
      echo -e "\n${CYAN}=== Pods — ingestion (Kafka / Strimzi / PostgreSQL / Debezium) ===${NC}"
      kubectl get pods -n "$KAFKA_NAMESPACE" -o wide
      echo -e "\n${CYAN}=== Pods — traitement (Apache Flink) ===${NC}"
      kubectl get pods -n "$FLINK_NAMESPACE" -o wide
      kubectl get flinkdeployment -n "$FLINK_NAMESPACE"
      echo -e "\n${CYAN}=== Pods — stockage (MinIO) ===${NC}"
      kubectl get pods -n "$MINIO_NAMESPACE" -o wide

      echo -e "\n${CYAN}=== Pipeline — message counts per Kafka topic ===${NC}"
      for topic in cdc.public.powercard_operations payments.decrypted payments.validated payments.normalized payments.dlq; do
        count=$(kubectl exec -n "$KAFKA_NAMESPACE" "${KAFKA_CLUSTER_NAME}-kafka-0" -- \
          bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$topic" \
          2>/dev/null | awk -F: '{sum+=$3} END {print sum+0}')
        printf "  %-45s %s messages\n" "$topic" "$count"
      done

      echo -e "\n${CYAN}=== MinIO — objets dans rt-payments ===${NC}"
      kubectl exec -n "$MINIO_NAMESPACE" deployment/minio -- \
        sh -c "mc alias set local http://localhost:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY >/dev/null 2>&1; \
               mc du --recursive local/$MINIO_BUCKET/ 2>/dev/null || \
               mc ls --recursive local/$MINIO_BUCKET/ 2>/dev/null | head -20"
      ;;
    help|--help|-h)
      usage
      ;;
    *)
      error "Commande inconnue : ${1}. Utilisez 'help' pour l'aide."
      ;;
  esac
}

main "$@"
