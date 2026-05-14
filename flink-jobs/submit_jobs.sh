#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
# HPS Real-Time Pipeline — Submit all 4 PyFlink jobs to Flink cluster
#
# Prerequisites
# ─────────────
#   1. Minikube running with the hps-flink:1.0 image already built:
#        eval $(minikube docker-env)
#        docker build -t hps-flink:1.0 ~/hps-rt-poc/flink-jobs/
#
#   2. Flink jobmanager + taskmanager deployments patched to use hps-flink:1.0
#      (see patch commands at the bottom of this header)
#
#   3. FERNET_KEY env var set:
#        export FERNET_KEY="<key printed by producer.py at startup>"
#
# Usage
# ─────
#   bash flink-jobs/submit_jobs.sh
#
# Patch deployments (run once after image build):
#   KUBECTL="minikube kubectl --"
#   $KUBECTL patch deployment flink-jobmanager -n flink --type=json \
#     -p='[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"hps-flink:1.0"}]'
#   $KUBECTL patch deployment flink-taskmanager -n flink --type=json \
#     -p='[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"hps-flink:1.0"}]'
#   $KUBECTL rollout status deployment/flink-jobmanager -n flink
#   $KUBECTL rollout status deployment/flink-taskmanager -n flink
# ══════════════════════════════════════════════════════════════════════════════
set -euo pipefail

KUBECTL="minikube kubectl --"
FLINK_NS="flink"
FLINK_REST="http://localhost:8081"
JOBS_DIR="/opt/jobs"
PF_PID=""

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }
section() { echo -e "\n${CYAN}══ $* ══${NC}"; }

# ── Cleanup port-forward on exit ──────────────────────────────────────────────
cleanup() {
    if [[ -n "$PF_PID" ]] && kill -0 "$PF_PID" 2>/dev/null; then
        kill "$PF_PID" 2>/dev/null || true
        info "Port-forward closed."
    fi
}
trap cleanup EXIT

# ── 1. Prerequisites ──────────────────────────────────────────────────────────
section "Checking prerequisites"

[[ -z "${FERNET_KEY:-}" ]] && error \
    "FERNET_KEY is not set.\n  export FERNET_KEY=\"<key from producer.py output>\""

minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running" \
    || error "Minikube is not running. Run: minikube start"

info "Minikube: running"
info "FERNET_KEY: set (${#FERNET_KEY} chars)"

# ── 2. Verify Flink pods are ready ────────────────────────────────────────────
section "Checking Flink cluster"

JM_POD=$($KUBECTL get pods -n "$FLINK_NS" \
    -l app=flink,component=jobmanager \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null) \
    || error "No flink-jobmanager pod found in namespace '$FLINK_NS'"

info "JobManager pod: $JM_POD"

$KUBECTL wait pod/"$JM_POD" -n "$FLINK_NS" \
    --for=condition=Ready --timeout=120s \
    && info "JobManager pod is Ready" \
    || error "JobManager pod did not become Ready in time"

# ── 3. Port-forward Flink REST API ────────────────────────────────────────────
section "Opening Flink REST API port-forward"

pkill -f "port-forward.*8081" 2>/dev/null || true
sleep 1

$KUBECTL port-forward "pod/$JM_POD" 8081:8081 -n "$FLINK_NS" \
    >/tmp/pf-flink.log 2>&1 &
PF_PID=$!

# Wait until the port-forward is accepting connections
for i in $(seq 1 15); do
    sleep 1
    if curl -sf "$FLINK_REST/overview" >/dev/null 2>&1; then
        info "Flink REST API reachable at $FLINK_REST"
        break
    fi
    [[ $i -eq 15 ]] && error "Flink REST API not reachable after 15 s. Check pf log: /tmp/pf-flink.log"
done

# ── 4. Verify Kafka topics are Ready ─────────────────────────────────────────
section "Verifying Kafka topics"

for topic in payments payments.decrypted payments.validated payments.normalized payments.dlq; do
    for attempt in $(seq 1 20); do
        status=$($KUBECTL get kafkatopic "$topic" -n kafka \
            -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
        if [[ "$status" == "True" ]]; then
            info "  Topic ready: $topic"
            break
        fi
        [[ $attempt -eq 20 ]] && error "Topic '$topic' not Ready after 60 s"
        sleep 3
    done
done

# ── 5. Submit jobs via flink run inside the JobManager pod ───────────────────
section "Submitting PyFlink jobs"

# Helper: submit one job and return the Flink job ID
submit_job() {
    local label="$1"
    local script="$2"

    info "Submitting $label ..."
    local output
    output=$($KUBECTL exec -n "$FLINK_NS" "$JM_POD" -- \
        flink run \
            --detached \
            --python "$JOBS_DIR/$script" \
            -Denv.java.opts="-DFERNET_KEY=${FERNET_KEY}" \
            -Dpython.client.executable=/usr/bin/python3 \
            -Dpython.executable=/usr/bin/python3 \
            -m localhost:8081 \
        2>&1)

    # Extract job ID from flink run output: "Job has been submitted with JobID <hex>"
    local job_id
    job_id=$(echo "$output" | grep -oP 'JobID \K[0-9a-f]{32}' || true)

    if [[ -z "$job_id" ]]; then
        warn "Could not parse JobID from output:"
        echo "$output"
    else
        info "  $label submitted — JobID: $job_id"
    fi
    echo "$job_id"
}

# Pass FERNET_KEY as a Flink dynamic property so it reaches the Python process
# via environment variable inside the TaskManager container.
# The jobs read it via os.getenv("FERNET_KEY").
$KUBECTL exec -n "$FLINK_NS" "$JM_POD" -- \
    bash -c "export FERNET_KEY='${FERNET_KEY}'" 2>/dev/null || true

JOB1_ID=$(submit_job "Job 1 — Decryption"     "job1_decrypt.py")
JOB2_ID=$(submit_job "Job 2 — Validation"     "job2_validate.py")
JOB3_ID=$(submit_job "Job 3 — Normalisation"  "job3_normalize.py")
JOB4_ID=$(submit_job "Job 4 — Optimisation"   "job4_optimize.py")

# ── 6. Verification summary ───────────────────────────────────────────────────
section "Pipeline status"

sleep 3  # give Flink a moment to register all jobs
RUNNING=$( curl -sf "$FLINK_REST/jobs" \
    | python3 -c "import sys,json; jobs=json.load(sys.stdin)['jobs']; \
      print(sum(1 for j in jobs if j['status']=='RUNNING'))")

echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
info "Jobs submitted:"
info "  Job 1 — Decryption    ${JOB1_ID:-<pending>}"
info "  Job 2 — Validation    ${JOB2_ID:-<pending>}"
info "  Job 3 — Normalisation ${JOB3_ID:-<pending>}"
info "  Job 4 — Optimisation  ${JOB4_ID:-<pending>}"
echo ""
info "Flink dashboard : $FLINK_REST"
info "Running jobs    : $RUNNING / 4"
echo ""
info "Verify MinIO layers:"
info "  bronze/ → http://localhost:9000/rt-payments"
info "  silver/ → http://localhost:9000/rt-payments"
info "  gold/   → http://localhost:9000/rt-payments"
echo ""
info "Send data with:"
info "  python3 scripts/producer.py --csv data/transactions.csv \\"
info "    --bootstrap localhost:9092 --rate 10 --limit 100"
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
