#!/usr/bin/env bash
# ── HPS Pipeline — Submit all 4 processing jobs ───────────────────────────────
# Usage:
#   export FERNET_KEY="<key printed by producer.py>"
#   bash flink-jobs/submit_jobs.sh
#
# Or pass the key inline:
#   FERNET_KEY="<key>" bash flink-jobs/submit_jobs.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECTL="minikube kubectl --"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── 1. Fernet key check ────────────────────────────────────────────────────────
if [ -z "${FERNET_KEY:-}" ]; then
  error "FERNET_KEY is not set.\nGet it from the last producer run:\n  python3 scripts/producer.py --csv data/transactions.csv ...\n  (printed at startup as: [producer] Fernet key (save to decrypt): ...)"
fi

# ── 2. Verify cluster is up ────────────────────────────────────────────────────
info "Checking Minikube..."
minikube status --format='{{.Host}}' | grep -q Running || error "Minikube is not running"

# ── 3. Kill any existing port-forwards ────────────────────────────────────────
info "Setting up port-forwards..."
pkill -f "port-forward.*9092" 2>/dev/null || true
pkill -f "port-forward.*9000" 2>/dev/null || true
sleep 1

# Kafka — broker pod direct (DNS monkeypatch handles metadata redirect)
$KUBECTL port-forward pod/hps-cluster-dual-role-0 9092:9092 -n kafka \
  >/tmp/pf-kafka.log 2>&1 &
# MinIO
$KUBECTL port-forward svc/minio 9000:9000 -n minio \
  >/tmp/pf-minio.log 2>&1 &

sleep 3
grep -q "Forwarding" /tmp/pf-kafka.log  || error "Kafka port-forward failed"
grep -q "Forwarding" /tmp/pf-minio.log  || error "MinIO port-forward failed"
info "Port-forwards active: Kafka=9092  MinIO=9000"

# ── 4. Wait for intermediate topics to be ready ────────────────────────────────
info "Waiting for intermediate Kafka topics..."
for topic in payments.decrypted payments.validated payments.normalized; do
  for i in $(seq 1 20); do
    ready=$($KUBECTL get kafkatopic "$topic" -n kafka \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
    [ "$ready" = "True" ] && break
    sleep 3
  done
  info "  Topic $topic: ready"
done

# ── 5. Export environment variables for jobs ──────────────────────────────────
export KAFKA_BOOTSTRAP="localhost:9092"
export MINIO_ENDPOINT="localhost:9000"
export MINIO_ACCESS_KEY="admin"
export MINIO_SECRET_KEY="admin123"
export MINIO_BUCKET="rt-payments"
export BATCH_SIZE="10"
export FERNET_KEY

# ── 6. Launch all 4 jobs ──────────────────────────────────────────────────────
info "Starting pipeline jobs..."

python3 "$SCRIPT_DIR/job1_decrypt.py"   --key "$FERNET_KEY" \
  >"$SCRIPT_DIR/job1.log" 2>&1 & JOB1=$!

python3 "$SCRIPT_DIR/job2_validate.py"  \
  >"$SCRIPT_DIR/job2.log" 2>&1 & JOB2=$!

python3 "$SCRIPT_DIR/job3_normalize.py" \
  >"$SCRIPT_DIR/job3.log" 2>&1 & JOB3=$!

python3 "$SCRIPT_DIR/job4_optimize.py"  \
  >"$SCRIPT_DIR/job4.log" 2>&1 & JOB4=$!

echo ""
echo "══════════════════════════════════════════════════════════════"
info "All 4 jobs running:"
info "  Job 1 decrypt   PID=$JOB1  log: flink-jobs/job1.log"
info "  Job 2 validate  PID=$JOB2  log: flink-jobs/job2.log"
info "  Job 3 normalize PID=$JOB3  log: flink-jobs/job3.log"
info "  Job 4 optimize  PID=$JOB4  log: flink-jobs/job4.log"
echo ""
info "Send data with:"
info "  python3 scripts/producer.py --csv data/transactions.csv \\"
info "    --bootstrap localhost:9092 --rate 10 --limit 100"
echo ""
info "Monitor logs:  tail -f flink-jobs/job*.log"
info "Stop all jobs: kill $JOB1 $JOB2 $JOB3 $JOB4"
echo "══════════════════════════════════════════════════════════════"

# ── 7. Wait for all jobs ──────────────────────────────────────────────────────
wait $JOB1 $JOB2 $JOB3 $JOB4
info "All jobs finished."
