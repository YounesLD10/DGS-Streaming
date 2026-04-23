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
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-6144}"
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
KAFKA_PARTITIONS=3

MINIO_BUCKET="rt-payments"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

FLINK_JOBMANAGER_REPLICAS=1
FLINK_TASKMANAGER_REPLICAS=2

TERRAFORM_DIR="$(pwd)/terraform"
K8S_MANIFESTS_DIR="$(pwd)/k8s"
SCRIPTS_DIR="$(pwd)/scripts"

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
variable "kafka_version"       { default = "$KAFKA_VERSION" }
variable "kafka_cluster_name"   { default = "$KAFKA_CLUSTER_NAME" }
variable "kafka_replicas"       { default = $KAFKA_REPLICAS }
variable "kafka_partitions"     { default = $KAFKA_PARTITIONS }
variable "topic_payments"       { default = "$KAFKA_TOPIC_PAYMENTS" }
variable "topic_dlq"            { default = "$KAFKA_TOPIC_DLQ" }
variable "minio_access_key"     { default = "$MINIO_ACCESS_KEY" }
variable "minio_secret_key"     { default = "$MINIO_SECRET_KEY" }
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

resource "kubernetes_manifest" "topic_payments" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = var.topic_payments
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = var.kafka_partitions
      replicas   = 1
      config     = { "retention.ms" = "604800000", "cleanup.policy" = "delete", "compression.type" = "lz4" }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}

resource "kubernetes_manifest" "topic_dlq" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = replace(var.topic_dlq, ".", "-")
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = 1
      replicas   = 1
      config     = { "retention.ms" = "2592000000", "cleanup.policy" = "delete" }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}
EOF

  # flink-crd.tf — FlinkDeployment
  cat > "$S2/flink-crd.tf" << 'EOF'
resource "kubernetes_manifest" "flink_deployment" {
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata   = { name = "poc-pipeline", namespace = var.traitement_namespace }
    spec = {
      image           = "flink:1.18-scala_2.12"
      flinkVersion    = "v1_18"
      imagePullPolicy = "IfNotPresent"
      serviceAccount  = "flink"
      flinkConfiguration = {
        "taskmanager.numberOfTaskSlots"                      = "4"
        "state.backend"                                      = "rocksdb"
        "state.checkpoints.dir"                              = "file:///tmp/flink-checkpoints"
        "execution.checkpointing.interval"                   = "60s"
        "execution.checkpointing.mode"                       = "EXACTLY_ONCE"
        "execution.checkpointing.min-pause"                  = "30s"
        "restart-strategy"                                   = "exponential-delay"
        "restart-strategy.exponential-delay.initial-backoff" = "1s"
        "restart-strategy.exponential-delay.max-backoff"     = "5min"
      }
      jobManager  = { resource = { memory = "1024m", cpu = 0.5 }, replicas = 1 }
      taskManager = { resource = { memory = "1024m", cpu = 1.0 }, replicas = 2 }
      mode        = "standalone"
    }
  }
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

# Génération du script producteur Python
generate_producer_script() {
  step "Génération du script Producer Python"
  mkdir -p "$SCRIPTS_DIR"

  cat > "$SCRIPTS_DIR/producer.py" << 'PYEOF'
#!/usr/bin/env python3
"""
Producer POC — lit le CSV Kaggle et publie sur le topic Kafka 'payments'
Chaque message est chiffré en base64 (simulation AES) et encodé en JSON.

Usage :
  pip install kafka-python pandas cryptography
  python producer.py --csv <fichier.csv> [--bootstrap <host:port>] [--rate <msg/s>]
"""
import argparse, base64, json, sys, time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from cryptography.fernet import Fernet

# Clé de chiffrement fixe pour le POC (à remplacer par KMS en prod)
_FERNET_KEY = b"dGhpcyBpcyBhIDMyLWJ5dGUga2V5AAAAAAAAAAAAA=="  # 32-byte placeholder
_fernet = Fernet(Fernet.generate_key())  # clé aléatoire par run en POC

def encrypt_payload(data: dict) -> str:
    raw = json.dumps(data).encode()
    return base64.b64encode(_fernet.encrypt(raw)).decode()

def build_envelope(row: dict, idx: int) -> dict:
    return {
        "eventId":   f"evt-{idx:08d}",
        "table":     "payments",
        "operation": "INSERT",
        "payload":   encrypt_payload(row),
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
    }

def main():
    parser = argparse.ArgumentParser(description="Kafka Payment Producer")
    parser.add_argument("--csv",       required=True,           help="Chemin vers le CSV Kaggle")
    parser.add_argument("--bootstrap", default="localhost:9094", help="Kafka bootstrap server")
    parser.add_argument("--topic",     default="payments",      help="Topic Kafka cible")
    parser.add_argument("--rate",      type=float, default=10,  help="Messages par seconde")
    parser.add_argument("--limit",     type=int,   default=0,   help="Limite de lignes (0=toutes)")
    args = parser.parse_args()

    print(f"[Producer] Lecture de {args.csv}")
    df = pd.read_csv(args.csv)
    if args.limit > 0:
        df = df.head(args.limit)
    print(f"[Producer] {len(df)} lignes chargées")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=10,
    )

    interval = 1.0 / args.rate if args.rate > 0 else 0
    sent = 0
    for i, row in df.iterrows():
        envelope = build_envelope(row.to_dict(), i)
        producer.send(args.topic, value=envelope)
        sent += 1
        if sent % 100 == 0:
            print(f"[Producer] {sent}/{len(df)} messages envoyés")
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    print(f"[Producer] ✓ {sent} messages publiés sur '{args.topic}'")

if __name__ == "__main__":
    main()
PYEOF

  chmod +x "$SCRIPTS_DIR/producer.py"
  success "Script producer.py généré dans $SCRIPTS_DIR"
}

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

    # ── Flink operator ───────────────────────────────────────────────────────
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

apply_terraform() {
  step "Application de l'infrastructure Terraform"

  # ── Étape 1 : tout sauf les ressources CRD-dépendantes ───────────────────
  # Le répertoire terraform/ ne contient PAS les manifests Kafka/Flink CRD :
  # ils sont dans terraform/stage2/ et n'existent pas encore sur disque.
  # => terraform plan/validate ne peut pas échouer sur des CRDs manquantes.
  cd "$TERRAFORM_DIR"
  log "terraform init (étape 1)"
  terraform init -upgrade
  _tf_import_if_missing "$TERRAFORM_DIR"
  log "terraform apply — étape 1 : namespaces, operators, attente CRDs"
  terraform apply -auto-approve
  cd - > /dev/null

  # ── Étape 2 : ressources CRD-dépendantes ─────────────────────────────────
  # generate_stage2_files() a déjà écrit terraform/stage2/ sur disque.
  # Les CRDs sont maintenant Established → le provider peut les valider.
  local S2="$TERRAFORM_DIR/stage2"
  cd "$S2"
  log "terraform init (étape 2 — stage2)"
  terraform init -upgrade
  log "terraform apply — étape 2 : Kafka cluster, topics, FlinkDeployment"
  terraform apply -auto-approve
  cd - > /dev/null

  success "Infrastructure Terraform appliquée (étapes 1 + 2)"
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

  log "Attente de Kafka (KafkaCluster ready)…"
  kubectl wait kafka/"$KAFKA_CLUSTER_NAME" \
    -n "$KAFKA_NAMESPACE" \
    --for=condition=Ready \
    --timeout=300s

  log "Attente de MinIO…"
  kubectl rollout status deployment/minio \
    -n "$MINIO_NAMESPACE" \
    --timeout=180s

  log "Attente de Flink JobManager…"
  kubectl wait pod \
    -l "app=poc-pipeline,component=jobmanager" \
    -n "$FLINK_NAMESPACE" \
    --for=condition=Ready \
    --timeout=300s || warn "JobManager pas encore prêt — vérifier avec: kubectl get pods -n $FLINK_NAMESPACE"
  success "Tous les composants sont opérationnels"
}

#Affichage du résumé
print_summary() {
  step "Résumé de l'infrastructure POC"

  MINIKUBE_IP=$(minikube ip 2>/dev/null || echo "<minikube-ip>")

  echo -e """
${GREEN}╔══════════════════════════════════════════════════════════╗
║          POC PIPELINE — Infrastructure prête             ║
╚══════════════════════════════════════════════════════════╝${NC}

${CYAN}Namespaces :${NC}
  kubectl get pods -n $KAFKA_NAMESPACE    # ingestion  (Kafka / Strimzi)
  kubectl get pods -n $FLINK_NAMESPACE    # traitement (Apache Flink)
  kubectl get pods -n $MINIO_NAMESPACE    # stockage   (MinIO)

${CYAN}Couche Ingestion — Kafka (Strimzi/KRaft) :${NC}
  Bootstrap interne : ${KAFKA_CLUSTER_NAME}-kafka-bootstrap.${KAFKA_NAMESPACE}.svc:9092
  Bootstrap externe : ${MINIKUBE_IP}:<nodeport>
  Topics            : $KAFKA_TOPIC_PAYMENTS, $KAFKA_TOPIC_DLQ

  # Lister les topics
  kubectl exec -n $KAFKA_NAMESPACE -it \$(kubectl get pod -n $KAFKA_NAMESPACE -l strimzi.io/name=${KAFKA_CLUSTER_NAME}-kafka -o jsonpath='{.items[0].metadata.name}') \\
    -- bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

${CYAN}Couche Traitement — Apache Flink :${NC}
  API REST : kubectl port-forward svc/poc-pipeline-rest 8081:8081 -n $FLINK_NAMESPACE
  UI       : http://localhost:8081

${CYAN}Couche Stockage — MinIO :${NC}
  Console  : kubectl port-forward svc/minio-console 9001:9001 -n $MINIO_NAMESPACE
  UI       : http://localhost:9001  (${MINIO_ACCESS_KEY} / ${MINIO_SECRET_KEY})
  Bucket   : $MINIO_BUCKET

${CYAN}Producer (local → couche ingestion) :${NC}
  # Exposer le port Kafka NodePort
  KAFKA_PORT=\$(kubectl get svc ${KAFKA_CLUSTER_NAME}-kafka-external-bootstrap \\
    -n $KAFKA_NAMESPACE -o jsonpath='{.spec.ports[0].nodePort}')
  python3 $SCRIPTS_DIR/producer.py \\
    --csv <votre_kaggle.csv> \\
    --bootstrap ${MINIKUBE_IP}:\$KAFKA_PORT \\
    --rate 50

${CYAN}Prochaines étapes :${NC}
  1. Compiler les 4 jobs Flink (Maven/Gradle) → JARs
  2. Soumettre via REST : POST http://localhost:8081/jars/upload
  3. Valider le flux bout-en-bout : Producer → Kafka → Flink → MinIO

${YELLOW}Arrêter l'environnement :${NC}
  cd $TERRAFORM_DIR && terraform destroy -auto-approve
  minikube stop
"""
}

# Nettoyage
teardown() {
  step "Teardown de l'infrastructure POC"
  warn "Cette action va détruire TOUTE l'infrastructure."
  read -r -p "Confirmer la destruction ? (yes/no) : " confirm
  [[ "$confirm" == "yes" ]] || { log "Annulé."; exit 0; }

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

#  Point d'entrée 
usage() {
  echo -e """
Usage : $0 [COMMANDE]

  up        Créer toute l'infrastructure (défaut)
  down      Détruire l'infrastructure
  plan      Afficher le plan Terraform sans appliquer
  status    Afficher le statut des pods
  help      Afficher cette aide
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
      generate_producer_script
      cleanup_before_deploy
      apply_terraform
      apply_k8s_extras
      wait_for_components
      print_summary
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
    status)
      echo -e "\n${CYAN}=== ingestion  (Kafka / Strimzi) — namespace: $KAFKA_NAMESPACE ===${NC}"
      kubectl get pods -n "$KAFKA_NAMESPACE" -o wide
      echo -e "\n${CYAN}=== traitement (Apache Flink)    — namespace: $FLINK_NAMESPACE ===${NC}"
      kubectl get pods -n "$FLINK_NAMESPACE" -o wide
      echo -e "\n${CYAN}=== stockage   (MinIO)           — namespace: $MINIO_NAMESPACE ===${NC}"
      kubectl get pods -n "$MINIO_NAMESPACE" -o wide
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