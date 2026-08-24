#!/usr/bin/env bash
# Stop the exporter and only the port-forwards recorded by start_business_metrics.sh.
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="$ROOT_DIR/.runtime/business-metrics"
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

info() { printf '%b\n' "${GREEN}[business-metrics]${RESET} $*"; }
warn() { printf '%b\n' "${YELLOW}[business-metrics]${RESET} $*" >&2; }

stop_recorded_process() {
    local name="$1" pid_file="$RUNTIME_DIR/$1.pid" pid command_line expected
    [[ -f "$pid_file" ]] || return 0
    pid="$(<"$pid_file")"
    command_line="$(ps -p "$pid" -o args= 2>/dev/null || true)"

    if [[ -n "$command_line" ]]; then
        if [[ "$name" == "exporter" ]]; then
            expected='hps_exporter.py'
        else
            expected='port-forward'
        fi
        if [[ "$command_line" == *"$expected"* ]]; then
            info "Stopping $name (PID $pid)"
            kill "$pid" 2>/dev/null || warn "$name (PID $pid) could not be stopped"
        else
            warn "PID $pid no longer belongs to the recorded $name process; leaving it untouched"
        fi
    fi
    rm -f "$pid_file"
}

stop_recorded_process exporter
stop_recorded_process kafka
stop_recorded_process minio
stop_recorded_process postgres

if [[ -d "$RUNTIME_DIR" ]]; then
    info "Runtime PID files removed; logs remain in $RUNTIME_DIR/logs"
fi
