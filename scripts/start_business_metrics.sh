#!/usr/bin/env bash
# Start the host-side SWAM business metrics exporter and its required forwards.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/business-metrics"
LOG_DIR="$RUNTIME_DIR/logs"
EXPORTER_PID_FILE="$RUNTIME_DIR/exporter.pid"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9094}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-}"
PG_DSN="${PG_DSN:-postgresql://hps:hps123@localhost:5432/datamart}"
LISTEN_PORT="${LISTEN_PORT:-8888}"
REFRESH_INTERVAL_SECONDS="${REFRESH_INTERVAL_SECONDS:-15}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-60}"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
RESET='\033[0m'
STARTED_PID_FILES=()

info() { printf '%b\n' "${GREEN}[business-metrics]${RESET} $*"; }
warn() { printf '%b\n' "${YELLOW}[business-metrics]${RESET} $*" >&2; }

cleanup_failed_start() {
    local pid_file pid
    for pid_file in "${STARTED_PID_FILES[@]:-}"; do
        [[ -f "$pid_file" ]] || continue
        pid="$(<"$pid_file")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$pid_file"
    done
}

die() {
    printf '%b\n' "${RED}[business-metrics] ERROR:${RESET} $*" >&2
    cleanup_failed_start
    exit 1
}

trap cleanup_failed_start ERR

for command in minikube curl python3 timeout pgrep nohup; do
    command -v "$command" >/dev/null 2>&1 || die "Required command not found: $command"
done

if [[ -f "$ROOT_DIR/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$ROOT_DIR/.venv/bin/activate"
    info "Activated .venv"
elif [[ -f "$ROOT_DIR/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$ROOT_DIR/venv/bin/activate"
    info "Activated venv"
fi

PYTHON_BIN="$(command -v python3)"
python_has_exporter_dependencies() {
    "$1" -c 'import kafka, minio, prometheus_client, psycopg2' >/dev/null 2>&1
}

if ! python_has_exporter_dependencies "$PYTHON_BIN"; then
    warn "$(basename "$PYTHON_BIN") from the activated virtual environment lacks exporter dependencies"
    if [[ -x /usr/bin/python3 ]] && python_has_exporter_dependencies /usr/bin/python3; then
        PYTHON_BIN=/usr/bin/python3
        warn "Using /usr/bin/python3, which has kafka-python, MinIO, Prometheus, and psycopg2 installed"
    else
        die "No Python interpreter has the exporter dependencies. Install kafka-python, minio, prometheus-client, and psycopg2-binary."
    fi
fi
mkdir -p "$LOG_DIR"

tcp_ready() {
    local port="$1"
    timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1
}

wait_for() {
    local description="$1"
    shift
    local elapsed=0
    until "$@" >/dev/null 2>&1; do
        if (( elapsed >= WAIT_TIMEOUT_SECONDS )); then
            die "Timed out waiting for $description after ${WAIT_TIMEOUT_SECONDS}s"
        fi
        sleep 1
        ((elapsed += 1))
    done
    info "$description is ready"
}

start_port_forward() {
    local name="$1" port="$2" namespace="$3" service="$4" mapping="$5"
    local pid_file="$RUNTIME_DIR/${name}.pid"

    if pgrep -f "port-forward.*svc/${service}.*${mapping}.*-n ${namespace}" >/dev/null 2>&1; then
        info "$name port-forward is already running"
        return
    fi

    if tcp_ready "$port"; then
        info "localhost:$port is already in use; reusing the existing service"
        return
    fi

    info "Starting $name port-forward on localhost:$port"
    nohup minikube kubectl -- port-forward "svc/$service" "$mapping" -n "$namespace" \
        >"$LOG_DIR/${name}.log" 2>&1 &
    echo "$!" >"$pid_file"
    STARTED_PID_FILES+=("$pid_file")
}

stop_existing_exporters() {
    local pids
    pids="$(pgrep -f '[p]ython(3)? .*scripts/hps_exporter\.py' || true)"
    if [[ -n "$pids" ]]; then
        warn "Stopping existing hps_exporter.py process(es): $pids"
        kill $pids 2>/dev/null || true
        sleep 1
    fi
    rm -f "$EXPORTER_PID_FILE"
}

export KAFKA_BOOTSTRAP MINIO_ENDPOINT MINIO_ACCESS_KEY MINIO_SECRET_KEY PG_DSN
export LISTEN_PORT REFRESH_INTERVAL_SECONDS

start_port_forward kafka 9094 kafka hps-cluster-kafka-bootstrap 9094:9092
start_port_forward minio 9000 minio minio 9000:9000
start_port_forward postgres 5432 kafka-connect postgres-datamart 5432:5432

wait_for "Kafka ($KAFKA_BOOTSTRAP)" tcp_ready 9094
wait_for "MinIO health endpoint" curl --fail --silent --max-time 2 http://localhost:9000/minio/health/live
wait_for "PostgreSQL ($PG_DSN)" "$PYTHON_BIN" -c 'import os, psycopg2; connection = psycopg2.connect(os.environ["PG_DSN"], connect_timeout=2); connection.close()'

stop_existing_exporters
info "Starting exporter on http://localhost:$LISTEN_PORT/metrics"
nohup "$PYTHON_BIN" "$ROOT_DIR/scripts/hps_exporter.py" >"$LOG_DIR/exporter.log" 2>&1 &
echo "$!" >"$EXPORTER_PID_FILE"
STARTED_PID_FILES+=("$EXPORTER_PID_FILE")

wait_for "exporter HTTP endpoint" curl --fail --silent --max-time 2 "http://localhost:$LISTEN_PORT/metrics"

required_metrics=(
    swam_payments_total
    swam_minio_objects
    swam_gold_transactions_total
    swam_gold_risk_score_total
    swam_gold_payment_channel_total
)
metrics=''
elapsed=0
while (( elapsed < WAIT_TIMEOUT_SECONDS )); do
    metrics="$(curl --fail --silent --max-time 2 "http://localhost:$LISTEN_PORT/metrics" || true)"
    missing=()
    for metric in "${required_metrics[@]}"; do
        grep -q "^${metric}" <<<"$metrics" || missing+=("$metric")
    done
    ((${#missing[@]} == 0)) && break
    sleep 1
    ((elapsed += 1))
done

if ((${#missing[@]} != 0)); then
    printf '%b\n' "${RED}[business-metrics] Exporter started, but required metrics are missing:${RESET} ${missing[*]}" >&2
    printf '%s\n' "Inspect $LOG_DIR/exporter.log and the port-forward logs in $LOG_DIR" >&2
    exit 1
fi

trap - ERR
printf '%b\n' "${GREEN}Business metrics stack is healthy: http://localhost:$LISTEN_PORT/metrics${RESET}"
