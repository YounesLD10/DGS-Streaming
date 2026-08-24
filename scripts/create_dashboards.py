"""
Create/update Grafana dashboards for the SWAM real-time payments streaming PoC.

Creates a Prometheus datasource (if missing) and pushes 6 dashboards via the
Grafana HTTP API:
  1. Executive Overview
  2. Flink Monitoring
  3. Kafka Monitoring
  4. Kubernetes Infrastructure
  5. MinIO Monitoring
  6. Business & Data Quality

Some panels reference hps_* business metrics (swam_payments_total,
swam_minio_objects, swam_datamart_total, swam_risk_score_total,
swam_payment_channel_total) which are not yet exported by Prometheus.
Those panels are still created and will show "No data" until the
exporter is deployed.
"""

import sys
import requests

GRAFANA_URL = "http://localhost:3000"
import os as _os
AUTH = (_os.getenv("GRAFANA_USER", "admin"), _os.getenv("GRAFANA_PASSWORD"))
if not AUTH[1]:
    import logging as _log
    _log.getLogger(__name__).warning("GRAFANA_PASSWORD env var not set — Grafana API calls will fail")
HEADERS = {"Content-Type": "application/json"}
PROM_URL = "http://prometheus.monitoring.svc:9090"

EPS = 1e-6  # used to express strict ">" thresholds with Grafana's >= step semantics


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------

def ensure_datasource():
    r = requests.get(f"{GRAFANA_URL}/api/datasources/name/Prometheus", auth=AUTH)
    if r.status_code == 200:
        uid = r.json()["uid"]
        print(f"Datasource 'Prometheus' already exists (uid={uid})")
        return uid

    payload = {
        "name": "Prometheus",
        "type": "prometheus",
        "url": PROM_URL,
        "access": "proxy",
        "isDefault": True,
    }
    r = requests.post(f"{GRAFANA_URL}/api/datasources", auth=AUTH, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()
    uid = data.get("datasource", {}).get("uid") or data.get("uid")
    print(f"Created datasource 'Prometheus' (uid={uid})")
    return uid


# ---------------------------------------------------------------------------
# Layout helper: simple left-to-right, top-to-bottom packer for a 24-col grid
# ---------------------------------------------------------------------------

class GridLayout:
    def __init__(self):
        self.x = 0
        self.y = 0
        self.row_h = 0

    def place(self, w, h):
        if self.x + w > 24:
            self.x = 0
            self.y += self.row_h
            self.row_h = 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos


# ---------------------------------------------------------------------------
# Panel builders
# ---------------------------------------------------------------------------

def thresholds(steps):
    """steps: list of (color, value) tuples; value=None marks the base step."""
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


def color_override(field_name, color):
    return {
        "matcher": {"id": "byName", "options": field_name},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}],
    }


def build_targets(ds_ref, queries, instant=False, fmt=None):
    targets = []
    for i, q in enumerate(queries):
        t = {"datasource": ds_ref, "expr": q["expr"], "refId": chr(ord("A") + i)}
        if q.get("legend"):
            t["legendFormat"] = q["legend"]
        if instant:
            t["instant"] = True
        if fmt:
            t["format"] = fmt
        targets.append(t)
    return targets


def stat_panel(title, ds_ref, queries, pos, unit="short", steps=None, fixed_color=None, color_mode="value"):
    defaults = {"unit": unit, "mappings": []}
    if fixed_color:
        defaults["color"] = {"mode": "fixed", "fixedColor": fixed_color}
    else:
        defaults["color"] = {"mode": "thresholds"}
    defaults["thresholds"] = thresholds(steps or [("green", None)])
    return {
        "type": "stat",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": color_mode,
            "graphMode": "area",
            "justifyMode": "auto",
        },
    }


def gauge_panel(title, ds_ref, queries, pos, unit="short", min_val=0, max_val=100, steps=None):
    defaults = {
        "unit": unit,
        "min": min_val,
        "max": max_val,
        "mappings": [],
        "color": {"mode": "thresholds"},
        "thresholds": thresholds(steps or [("green", None)]),
    }
    return {
        "type": "gauge",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "orientation": "auto",
            "showThresholdLabels": False,
            "showThresholdMarkers": True,
        },
    }


def timeseries_panel(title, ds_ref, queries, pos, unit="short", steps=None, overrides=None,
                      axis_label=None, fill_opacity=10):
    custom = {
        "drawStyle": "line",
        "lineWidth": 1,
        "fillOpacity": fill_opacity,
        "showPoints": "never",
        "spanNulls": True,
    }
    if axis_label:
        custom["axisLabel"] = axis_label
    defaults = {"unit": unit, "mappings": [], "custom": custom}
    if steps:
        defaults["thresholds"] = thresholds(steps)
        custom["thresholdsStyle"] = {"mode": "line"}
    return {
        "type": "timeseries",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries),
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
    }


def barchart_panel(title, ds_ref, queries, pos, unit="short", steps=None, overrides=None):
    defaults = {"unit": unit, "mappings": [], "custom": {}}
    if steps:
        defaults["thresholds"] = thresholds(steps)
        defaults["color"] = {"mode": "thresholds"}
    return {
        "type": "barchart",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries, instant=True),
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom"},
            "orientation": "auto",
            "xTickLabelRotation": 0,
        },
    }


def piechart_panel(title, ds_ref, queries, pos, overrides=None):
    return {
        "type": "piechart",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries, instant=True),
        "fieldConfig": {"defaults": {"unit": "short", "mappings": []}, "overrides": overrides or []},
        "options": {
            "legend": {"displayMode": "list", "placement": "right", "values": ["value"]},
            "pieType": "pie",
            "tooltip": {"mode": "single"},
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        },
    }


def table_panel(title, ds_ref, queries, pos, rename=None):
    return {
        "type": "table",
        "title": title,
        "gridPos": pos,
        "datasource": ds_ref,
        "targets": build_targets(ds_ref, queries, instant=True, fmt="table"),
        "fieldConfig": {"defaults": {"unit": "short", "mappings": []}, "overrides": []},
        "options": {"showHeader": True, "cellHeight": "sm"},
        "transformations": [
            {
                "id": "organize",
                "options": {
                    "excludeByName": {"__name__": True, "instance": True, "job": True},
                    "renameByName": rename or {},
                },
            }
        ],
    }


DEFAULT_SIZE = {
    "stat": (6, 4),
    "gauge": (6, 4),
    "timeseries": (12, 8),
    "barchart": (12, 8),
    "piechart": (12, 8),
    "table": (24, 8),
}


def build_panel(spec, ds_ref, pos):
    t = spec["type"]
    queries = spec["queries"]
    overrides = [color_override(name, color) for name, color in spec.get("overrides", [])]
    if t == "stat":
        return stat_panel(spec["title"], ds_ref, queries, pos, unit=spec.get("unit", "short"),
                           steps=spec.get("steps"), fixed_color=spec.get("fixed_color"),
                           color_mode=spec.get("color_mode", "value"))
    if t == "gauge":
        return gauge_panel(spec["title"], ds_ref, queries, pos, unit=spec.get("unit", "short"),
                            min_val=spec.get("min", 0), max_val=spec.get("max", 100),
                            steps=spec.get("steps"))
    if t == "timeseries":
        return timeseries_panel(spec["title"], ds_ref, queries, pos, unit=spec.get("unit", "short"),
                                 steps=spec.get("steps"), overrides=overrides,
                                 axis_label=spec.get("axis_label"),
                                 fill_opacity=spec.get("fill_opacity", 10))
    if t == "barchart":
        return barchart_panel(spec["title"], ds_ref, queries, pos, unit=spec.get("unit", "short"),
                               steps=spec.get("steps"), overrides=overrides)
    if t == "piechart":
        return piechart_panel(spec["title"], ds_ref, queries, pos, overrides=overrides)
    if t == "table":
        return table_panel(spec["title"], ds_ref, queries, pos, rename=spec.get("rename"))
    raise ValueError(f"unknown panel type: {t}")


def build_dashboard(uid, title, specs, ds_ref):
    layout = GridLayout()
    panels = []
    for spec in specs:
        w, h = spec.get("size", DEFAULT_SIZE[spec["type"]])
        panels.append(build_panel(spec, ds_ref, layout.place(w, h)))
    return {
        "dashboard": {
            "uid": uid,
            "title": title,
            "tags": ["swam", "streaming", "poc"],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"},
            "panels": panels,
        },
        "overwrite": True,
    }


# ---------------------------------------------------------------------------
# Dashboard 1: Executive Overview
# ---------------------------------------------------------------------------

def dashboard_executive_overview():
    return [
        {"title": "Flink Jobs Running", "type": "stat",
         "queries": [{"expr": "flink_jobmanager_numRunningJobs"}],
         "steps": [("red", None), ("orange", 2), ("green", 4)]},

        {"title": "Total Processed", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="gold_enriched"}'}],
         "steps": [("red", None), ("orange", EPS), ("green", 100 + EPS)]},

        {"title": "DLQ Count", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "steps": [("green", None), ("orange", 10 + EPS), ("red", 50 + EPS)],
         "fixed_color": "red", "color_mode": "background"},

        {"title": "Rejection Rate %", "type": "gauge",
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 20), ("red", 30)]},

        {"title": "Bronze Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="bronze"}'}],
         "fixed_color": "orange", "color_mode": "background"},

        {"title": "Silver Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="silver"}'}],
         "fixed_color": "blue", "color_mode": "background"},

        {"title": "Gold Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="gold"}'}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Pipeline Stage Flow", "type": "timeseries",
         "queries": [
             {"expr": 'swam_payments_total{stage="raw_encrypted"}', "legend": "Encrypted"},
             {"expr": 'swam_payments_total{stage="decrypted"}', "legend": "Decrypted"},
             {"expr": 'swam_payments_total{stage="validated"}', "legend": "Validated"},
             {"expr": 'swam_payments_total{stage="gold_enriched"}', "legend": "Gold Enriched"},
             {"expr": 'swam_payments_total{stage="dead_letter"}', "legend": "Dead Letter"},
         ],
         "overrides": [
             ("Encrypted", "blue"), ("Decrypted", "#22d3ee"), ("Validated", "#a3e635"),
             ("Gold Enriched", "green"), ("Dead Letter", "red"),
         ]},

        {"title": "Global Throughput (tx/min)", "type": "timeseries",
         "queries": [{"expr": 'rate(swam_payments_total{stage="gold_enriched"}[5m]) * 60',
                       "legend": "Throughput"}],
         "fill_opacity": 25, "overrides": [("Throughput", "green")]},
    ]


# ---------------------------------------------------------------------------
# Dashboard 2: Flink Monitoring
# ---------------------------------------------------------------------------

def dashboard_flink_monitoring():
    return [
        {"title": "Jobs Running", "type": "stat",
         "queries": [{"expr": "flink_jobmanager_numRunningJobs"}]},

        {"title": "Task Slots Total", "type": "stat",
         "queries": [{"expr": "flink_jobmanager_taskSlotsTotal"}],
         "steps": [("red", None), ("orange", 4), ("green", 6)]},

        {"title": "Task Slots Available", "type": "stat",
         "queries": [{"expr": "flink_jobmanager_taskSlotsAvailable"}],
         "steps": [("red", None), ("green", EPS)]},

        {"title": "Restart Count", "type": "stat",
         "queries": [{"expr": "sum(flink_jobmanager_job_numRestarts)"}],
         "steps": [("green", None), ("orange", 1), ("red", 3)]},

        {"title": "Records In/sec", "type": "timeseries",
         "queries": [{"expr": "rate(flink_taskmanager_job_task_operator_numRecordsIn[1m])",
                       "legend": "{{task_name}}"}],
         "axis_label": "records/s"},

        {"title": "Records Out/sec", "type": "timeseries",
         "queries": [{"expr": "rate(flink_taskmanager_job_task_operator_numRecordsOut[1m])",
                       "legend": "{{task_name}}"}],
         "axis_label": "records/s"},

        {"title": "Backpressure Ratio", "type": "timeseries",
         "queries": [{"expr": "flink_taskmanager_job_task_isBackPressured",
                       "legend": "{{task_name}}"}],
         "steps": [("green", None), ("orange", 0.5), ("red", 0.8)]},

        {"title": "Busy Time Ratio", "type": "timeseries",
         "queries": [{"expr": "rate(flink_taskmanager_job_task_busyTimeMsPerSecond[1m]) / 1000",
                       "legend": "{{task_name}}"}],
         "unit": "percentunit"},

        {"title": "Checkpoint Duration", "type": "timeseries",
         "queries": [{"expr": "flink_jobmanager_job_lastCheckpointDuration",
                       "legend": "{{job_name}}"}],
         "unit": "ms",
         "steps": [("green", None), ("orange", 5000), ("red", 10000)]},

        {"title": "Checkpoint Size", "type": "timeseries",
         "queries": [{"expr": "flink_jobmanager_job_lastCheckpointSize",
                       "legend": "{{job_name}}"}],
         "unit": "bytes"},

        {"title": "Checkpoint Failures", "type": "stat",
         "queries": [{"expr": "flink_jobmanager_job_numberOfFailedCheckpoints"}],
         "steps": [("green", None), ("red", EPS)]},

        {"title": "JVM Heap Usage %", "type": "timeseries",
         "queries": [{"expr": "flink_taskmanager_Status_JVM_Memory_Heap_Used / "
                               "flink_taskmanager_Status_JVM_Memory_Heap_Max * 100",
                       "legend": "{{tm_id}}"}],
         "unit": "percent",
         "steps": [("green", None), ("orange", 85), ("red", 95)]},

        {"title": "CPU Load", "type": "gauge",
         "queries": [{"expr": "flink_taskmanager_Status_JVM_CPU_Load * 100"}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 80), ("red", 90)]},
    ]


# ---------------------------------------------------------------------------
# Dashboard 3: Kafka Monitoring
# ---------------------------------------------------------------------------

def dashboard_kafka_monitoring():
    return [
        {"title": "Messages per Topic", "type": "barchart",
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}]},

        {"title": "DLQ Activity", "type": "timeseries",
         "queries": [{"expr": 'rate(swam_payments_total{stage="dead_letter"}[5m])',
                       "legend": "DLQ rate"}],
         "overrides": [("DLQ rate", "red")]},

        {"title": "Topic Offsets", "type": "table",
         "queries": [{"expr": "swam_payments_total"}],
         "rename": {"stage": "Topic", "Value": "Messages", "Time": "Last Update"}},

        {"title": "Producer Rate", "type": "timeseries",
         "queries": [{"expr": 'rate(swam_payments_total{stage="raw_encrypted"}[1m])',
                       "legend": "Producer"}],
         "axis_label": "msg/s"},

        {"title": "Consumer Rate", "type": "timeseries",
         "queries": [{"expr": 'rate(swam_payments_total{stage="gold_enriched"}[1m])',
                       "legend": "Consumer"}],
         "axis_label": "msg/s"},
    ]


# ---------------------------------------------------------------------------
# Dashboard 4: Kubernetes Infrastructure
# ---------------------------------------------------------------------------

def dashboard_kubernetes_infrastructure():
    ns = 'namespace=~"kafka|flink|minio|kafka-connect|monitoring"'
    return [
        {"title": "Running Pods", "type": "stat",
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Running"})'}],
         "steps": [("red", None), ("orange", 12), ("green", 18)]},

        {"title": "Pending Pods", "type": "stat",
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Pending"})'}],
         "steps": [("green", None), ("orange", 1), ("red", 3)]},

        {"title": "Failed Pods", "type": "stat",
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Failed"})'}],
         "steps": [("green", None), ("red", EPS)]},

        {"title": "CPU per Namespace", "type": "timeseries",
         "queries": [{"expr": f"sum(rate(container_cpu_usage_seconds_total{{{ns}}}[2m])) by (namespace)",
                       "legend": "{{namespace}}"}],
         "axis_label": "cores"},

        {"title": "Memory per Namespace", "type": "timeseries",
         "queries": [{"expr": f"sum(container_memory_working_set_bytes{{{ns}}}) by (namespace)",
                       "legend": "{{namespace}}"}],
         "unit": "bytes"},

        {"title": "Pod Restarts", "type": "barchart",
         "queries": [{"expr": "sum(kube_pod_container_status_restarts_total) by (pod, namespace)",
                       "legend": "{{pod}}"}],
         "steps": [("green", None), ("orange", 1), ("red", 3)]},

        {"title": "Network I/O", "type": "timeseries",
         "queries": [
             {"expr": 'sum(rate(container_network_receive_bytes_total{namespace=~"kafka|flink"}[2m])) by (namespace)',
              "legend": "{{namespace}} rx"},
             {"expr": 'sum(rate(container_network_transmit_bytes_total{namespace=~"kafka|flink"}[2m])) by (namespace)',
              "legend": "{{namespace}} tx"},
         ],
         "unit": "Bps"},
    ]


# ---------------------------------------------------------------------------
# Dashboard 5: MinIO Monitoring
# ---------------------------------------------------------------------------

def dashboard_minio_monitoring():
    return [
        {"title": "Bronze Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="bronze"}'}],
         "fixed_color": "orange", "color_mode": "background"},

        {"title": "Silver Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="silver"}'}],
         "fixed_color": "blue", "color_mode": "background"},

        {"title": "Gold Objects", "type": "stat",
         "queries": [{"expr": 'swam_minio_objects{layer="gold"}'}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Total Storage Used", "type": "gauge",
         "queries": [{"expr": 'minio_cluster_usage_total_bytes'}],
         "unit": "bytes", "min": 0, "max": 10737418240},

        {"title": "Objects Growth", "type": "timeseries",
         "queries": [{"expr": 'swam_minio_objects{layer=~"bronze|silver|gold"}',
                       "legend": "{{layer}}"}]},

        {"title": "MinIO API Requests", "type": "timeseries",
         "queries": [{"expr": "rate(minio_s3_requests_total[1m])", "legend": "{{api}}"}],
         "axis_label": "req/s"},

        {"title": "Erreurs MinIO (toutes API)", "type": "stat",
         "queries": [{"expr": "sum(rate(minio_s3_requests_errors_total[5m]))"}],
         "steps": [("green", None), ("orange", 1), ("red", 10)]},
    ]


# ---------------------------------------------------------------------------
# Dashboard 6: Business & Data Quality
# ---------------------------------------------------------------------------

def dashboard_business_data_quality():
    return [
        {"title": "Valid Transactions", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="validated"}'}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Invalid Transactions", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red", "color_mode": "background"},

        {"title": "Rejection Rate", "type": "gauge",
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 20), ("red", 30)]},

        {"title": "Risk Score Distribution", "type": "piechart",
         "queries": [{"expr": "swam_gold_risk_score_total", "legend": "{{risk}}"}],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},

        {"title": "Payment Channel Distribution", "type": "piechart",
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},

        {"title": "DataMart Total Rows (gold_transactions, live)", "type": "stat",
         "queries": [{"expr": "swam_gold_transactions_total"}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Risk Score Time Series", "type": "timeseries",
         "queries": [
             {"expr": 'swam_gold_risk_score_total{risk="HIGH"}', "legend": "HIGH"},
             {"expr": 'swam_gold_risk_score_total{risk="MEDIUM"}', "legend": "MEDIUM"},
             {"expr": 'swam_gold_risk_score_total{risk="LOW"}', "legend": "LOW"},
         ],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},

        {"title": "Channel Time Series", "type": "timeseries",
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

DASHBOARDS = [
    ("swam-exec-overview", "SWAM - Executive Overview", dashboard_executive_overview),
    ("swam-flink-monitoring", "SWAM - Flink Monitoring", dashboard_flink_monitoring),
    ("swam-kafka-monitoring", "SWAM - Kafka Monitoring", dashboard_kafka_monitoring),
    ("swam-k8s-infrastructure", "SWAM - Kubernetes Infrastructure", dashboard_kubernetes_infrastructure),
    ("swam-minio-monitoring", "SWAM - MinIO Monitoring", dashboard_minio_monitoring),
    ("swam-business-quality", "SWAM - Business & Data Quality", dashboard_business_data_quality),
]


def main():
    ds_uid = ensure_datasource()
    ds_ref = {"type": "prometheus", "uid": ds_uid}

    results = []
    for uid, title, builder in DASHBOARDS:
        payload = build_dashboard(uid, title, builder(), ds_ref)
        r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=AUTH, headers=HEADERS, json=payload)
        if r.status_code == 200:
            data = r.json()
            url = f"{GRAFANA_URL}{data['url']}"
            print(f"OK   {title} -> {url}")
            results.append((title, url, "OK"))
        else:
            print(f"FAIL {title} -> HTTP {r.status_code}: {r.text}")
            results.append((title, None, f"FAIL ({r.status_code})"))

    print("\n--- Verifying Grafana home dashboard ---")
    r = requests.get(f"{GRAFANA_URL}/api/dashboards/home", auth=AUTH)
    print(f"GET /api/dashboards/home -> HTTP {r.status_code}")

    print("\n--- Summary ---")
    for title, url, status in results:
        print(f"{status:12s} {title:35s} {url or ''}")

    if any(status != "OK" for _, _, status in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
