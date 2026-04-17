#!/usr/bin/env bash
# ── HPS Real-Time PoC — Full Setup Script ─────────────────────────────────────
# Requires: minikube (running), helm, terraform
# On this machine kubectl must be used as: minikube kubectl --
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
KUBECTL="minikube kubectl --"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── 1. Prerequisite check ──────────────────────────────────────────────────────
info "Checking prerequisites..."
minikube status --format='{{.Host}}' | grep -q Running || \
  error "Minikube is not running. Start with: minikube start --cpus=2 --memory=3072 --driver=docker"
command -v helm      >/dev/null 2>&1 || error "helm not found"
command -v terraform >/dev/null 2>&1 || error "terraform not found"
info "All prerequisites satisfied."

# ── 2. Add Helm repositories ───────────────────────────────────────────────────
info "Adding Helm repositories..."
helm repo add strimzi https://strimzi.io/charts/     2>/dev/null || true
helm repo add minio   https://charts.min.io/         2>/dev/null || true
helm repo update
info "Helm repositories updated."

# ── 3. Terraform init + apply ──────────────────────────────────────────────────
info "Running terraform init..."
cd "$PROJECT_ROOT/terraform"
terraform init -upgrade

info "Running terraform apply..."
terraform apply -auto-approve

# ── 4. Apply k8s/ manifests ────────────────────────────────────────────────────
info "Applying Kubernetes manifests (RBAC + NetworkPolicies)..."
$KUBECTL apply -f "$PROJECT_ROOT/k8s/flink-rbac.yaml"
$KUBECTL apply -f "$PROJECT_ROOT/k8s/network-policies.yaml"

# ── 5. Wait for all pods ───────────────────────────────────────────────────────
info "Waiting for all pods to be Running..."

wait_ns() {
  local ns="$1"
  local timeout=300
  info "  Waiting for namespace: $ns"
  $KUBECTL wait pods --all -n "$ns" --for=condition=Ready --timeout="${timeout}s" 2>/dev/null || \
    warn "  Some pods in $ns not Ready after ${timeout}s — check: $KUBECTL get pods -n $ns"
}

wait_ns kafka
wait_ns flink
wait_ns minio

# ── 6. Final status ────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════"
info "Final cluster status:"
echo ""
echo "── kafka ──────────────────────────────────────────────────────"
$KUBECTL get pods -n kafka
echo ""
echo "── flink ──────────────────────────────────────────────────────"
$KUBECTL get pods -n flink
echo ""
echo "── minio ──────────────────────────────────────────────────────"
$KUBECTL get pods -n minio
echo ""
echo "── Kafka topics ───────────────────────────────────────────────"
$KUBECTL get kafkatopic -n kafka 2>/dev/null || true
echo ""
echo "── Terraform outputs ──────────────────────────────────────────"
terraform -chdir="$PROJECT_ROOT/terraform" output
echo "══════════════════════════════════════════════════════════════"
info "PoC setup complete!"
info "To run the producer:"
info "  pip install kafka-python cryptography pandas"
info "  python scripts/producer.py --csv /path/to/creditcard.csv"
