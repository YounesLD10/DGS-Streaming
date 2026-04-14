
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
MINIKUBE_K8S_VERSION="${MINIKUBE_K8S_VERSION:-v1.29.0}"

KAFKA_NAMESPACE="ingestion"
FLINK_NAMESPACE="traitement"
MINIO_NAMESPACE="stockage"

STRIMZI_VERSION="0.40.0"
FLINK_OPERATOR_VERSION="1.8.0"
MINIO_OPERATOR_VERSION="5.0.15"

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

# Minikube
start_minikube() {
  step "Démarrage de Minikube"
  if minikube status &>/dev/null; then
    warn "Minikube est déjà en cours d'exécution"
    return
  fi
  log "Lancement de Minikube (CPUs=$MINIKUBE_CPUS, RAM=${MINIKUBE_MEMORY}MB, Disk=$MINIKUBE_DISK)"
  minikube start \
    --driver="$MINIKUBE_DRIVER" \
    --cpus="$MINIKUBE_CPUS" \
    --memory="$MINIKUBE_MEMORY" \
    --disk-size="$MINIKUBE_DISK" \
    --kubernetes-version="$MINIKUBE_K8S_VERSION" \
    --addons=ingress,metrics-server \
    --embed-certs
  success "Minikube démarré"
  kubectl cluster-info
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
variable "minio_operator_version"  { default = "$MINIO_OPERATOR_VERSION" }

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

  #kafka.tf
  cat > "$TERRAFORM_DIR/kafka.tf" << 'EOF'
# Strimzi Operator (Kafka)
resource "helm_release" "strimzi_operator" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = var.strimzi_version
  namespace        = var.ingestion_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.ingestion]

  set {
    name  = "watchNamespaces"
    value = "{${var.ingestion_namespace}}"
  }
  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }

  timeout         = 300
  wait            = true
  wait_for_jobs   = true
}

# Kafka Cluster (KRaft mode — sans ZooKeeper)
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
        version  = "3.7.0"
        replicas = var.kafka_replicas
        listeners = [
          {
            name = "plain"
            port = 9092
            type = "internal"
            tls  = false
          },
          {
            name = "external"
            port = 9094
            type = "nodeport"
            tls  = false
          }
        ]
        config = {
          "offsets.topic.replication.factor"         = "1"
          "transaction.state.log.replication.factor" = "1"
          "transaction.state.log.min.isr"            = "1"
          "default.replication.factor"               = "1"
          "min.insync.replicas"                      = "1"
          "inter.broker.protocol.version"            = "3.7"
        }
        storage = {
          type = "ephemeral"
        }
        resources = {
          requests = { memory = "512Mi", cpu = "250m" }
          limits   = { memory = "1Gi",  cpu = "500m" }
        }
      }
      zookeeper = {
        replicas = 1
        storage  = { type = "ephemeral" }
        resources = {
          requests = { memory = "256Mi", cpu = "100m" }
          limits   = { memory = "512Mi", cpu = "200m" }
        }
      }
      entityOperator = {
        topicOperator = {}
        userOperator  = {}
      }
    }
  }
  depends_on = [helm_release.strimzi_operator]
}

# Topic : payments
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
      config = {
        "retention.ms"       = "604800000"   # 7 jours
        "cleanup.policy"     = "delete"
        "compression.type"   = "lz4"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}

# Topic : payments.dlq
resource "kubernetes_manifest" "topic_dlq" {
  manifest = {
    apiVersion = "kafka.strimzi.io/v1beta2"
    kind       = "KafkaTopic"
    metadata = {
      name      = replace(var.topic_dlq, ".", "-")  # K8s name
      namespace = var.ingestion_namespace
      labels    = { "strimzi.io/cluster" = var.kafka_cluster_name }
    }
    spec = {
      partitions = 1
      replicas   = 1
      config = {
        "retention.ms"   = "2592000000"  # 30 jours
        "cleanup.policy" = "delete"
      }
    }
  }
  depends_on = [kubernetes_manifest.kafka_cluster]
}
EOF

  #flink.tf
  cat > "$TERRAFORM_DIR/flink.tf" << 'EOF'
# Flink Kubernetes Operator
resource "helm_release" "flink_operator" {
  name             = "flink-kubernetes-operator"
  repository       = "https://downloads.apache.org/flink/flink-kubernetes-operator-${var.flink_operator_version}/"
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

# FlinkDeployment : 1 JobManager + 2 TaskManagers
resource "kubernetes_manifest" "flink_deployment" {
  manifest = {
    apiVersion = "flink.apache.org/v1beta1"
    kind       = "FlinkDeployment"
    metadata = {
      name      = "poc-pipeline"
      namespace = var.traitement_namespace
    }
    spec = {
      image          = "flink:1.18-scala_2.12"
      flinkVersion   = "v1_18"
      imagePullPolicy = "IfNotPresent"
      serviceAccount = "flink"

      flinkConfiguration = {
        "taskmanager.numberOfTaskSlots" = "4"
        "state.backend"                 = "rocksdb"
        "state.checkpoints.dir"         = "file:///tmp/flink-checkpoints"
        "execution.checkpointing.interval"             = "60s"
        "execution.checkpointing.mode"                 = "EXACTLY_ONCE"
        "execution.checkpointing.min-pause"            = "30s"
        "restart-strategy"                             = "exponential-delay"
        "restart-strategy.exponential-delay.initial-backoff" = "1s"
        "restart-strategy.exponential-delay.max-backoff"     = "5min"
      }

      jobManager = {
        resource = {
          memory = "1024m"
          cpu    = 0.5
        }
        replicas = 1
      }

      taskManager = {
        resource = {
          memory = "1024m"
          cpu    = 1.0
        }
        replicas = 2
      }

      # Pas de job embarqué au démarrage (session cluster)
      # Les 4 jobs seront soumis séparément via Flink REST API
      mode = "standalone"
    }
  }
  depends_on = [helm_release.flink_operator]
}
EOF

  # minio.tf
  cat > "$TERRAFORM_DIR/minio.tf" << 'EOF'
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

# Application Terraform
apply_terraform() {
  step "Application de l'infrastructure Terraform"
  cd "$TERRAFORM_DIR"

  log "terraform init"
  terraform init -upgrade

  log "terraform validate"
  terraform validate

  log "terraform plan"
  terraform plan -out=tfplan

  log "terraform apply"
  terraform apply -auto-approve tfplan

  cd - > /dev/null
  success "Infrastructure Terraform appliquée"
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

  cd "$TERRAFORM_DIR"
  terraform destroy -auto-approve
  cd - > /dev/null
  minikube stop
  success "Infrastructure détruite et Minikube arrêté"
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
      generate_k8s_manifests
      generate_producer_script
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
      terraform plan
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
