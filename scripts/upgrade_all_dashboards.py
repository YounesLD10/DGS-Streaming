"""
Upgrade the 7 remaining SWAM dashboards (Flink already done separately in
upgrade_flink_dashboard.py). Same UIDs, overwrite=True — the 8 dashboards
stay at 8, nothing is created or deleted.

Design notes / deviations from the inspiration dashboards, confirmed by
querying Prometheus live before writing any panel:

- Kafka (insp. 24565): kafka_* and strimzi_* metrics do not exist at all in
  this Prometheus (0 series) -- the Strimzi KRaft cluster has no JMX
  exporter sidecar. Native cluster-health/consumer-lag panels are replaced
  with an explanatory text panel; pipeline throughput uses swam_payments_total
  (the custom business exporter) as the only real proxy for Kafka activity.
- Kubernetes (insp. 15661): container_cpu_usage_seconds_total /
  container_memory_working_set_bytes on this single-node cAdvisor have NO
  `container` label at all (cgroup is aggregated at pod level), so the
  `container!=""` filter from the brief returns empty -- dropped it and kept
  the already-working by-namespace queries. container_network_* has only
  2 node-level series (id="/", interfaces bridge/eth0), no namespace/pod
  labels -- Network I/O stays masked with an explanatory panel (confirmed
  cAdvisor limitation, not a config bug).
- MinIO (13502 returned 404 on grafana.com, dashboard ID withdrawn/private)
  -- built from scratch directly off the 77 real minio_* metrics exposed by
  this cluster's own /minio/v2/metrics/cluster endpoint.
- Every ratio panel (Rejection Rate, Data Quality Score, risk %) wraps BOTH
  sides of the division in sum() -- dividing two vectors with different
  label sets (e.g. risk="HIGH" vs no filter) returns empty otherwise; this
  is the same vector-matching bug fixed earlier in the other dashboards.

verify_query() re-checks every panel's expr against the live Prometheus
before the dashboard is built; any panel whose query returns no series is
dropped and reported, never silently included or replaced by a fabricated
metric.
"""

import os
import sys

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from create_dashboards import (  # noqa: E402
    AUTH,
    DEFAULT_SIZE,
    GRAFANA_URL,
    HEADERS,
    GridLayout,
    build_panel,
    ensure_datasource,
)

PROM_QUERY_URL = "http://localhost:9090/api/v1/query"

EPS = 1e-6


def verify_query(expr):
    try:
        r = requests.get(PROM_QUERY_URL, params={"query": expr}, timeout=5)
        r.raise_for_status()
        result = r.json().get("data", {}).get("result", [])
        return len(result) > 0
    except Exception:
        return False


def row_panel(title, pos):
    return {"type": "row", "title": title, "gridPos": pos, "collapsed": False, "panels": []}


def text_panel(title, content, pos):
    return {
        "type": "text",
        "title": title,
        "gridPos": pos,
        "options": {"mode": "markdown", "content": content},
    }


def build_dashboard(uid, title, description, tags, specs, ds_ref):
    """specs: list of dicts, each either {"row": "..."}, {"text": "...", "content": "..."}
    or a normal build_panel spec. Panels whose query returns no data are
    dropped and reported (never replaced by a fabricated metric)."""
    layout = GridLayout()
    panels = []
    masked = []
    for spec in specs:
        if "row" in spec:
            panels.append(row_panel(spec["row"], layout.place(24, 1)))
            continue
        if "text" in spec:
            w, h = spec.get("size", (24, 4))
            panels.append(text_panel(spec["text"], spec["content"], layout.place(w, h)))
            continue
        ok = all(verify_query(q["expr"]) for q in spec["queries"])
        if not ok:
            masked.append(spec["title"])
            continue
        w, h = spec.get("size", DEFAULT_SIZE[spec["type"]])
        panels.append(build_panel(spec, ds_ref, layout.place(w, h)))
    return {
        "dashboard": {
            "uid": uid,
            "title": title,
            "description": description,
            "tags": tags,
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"},
            "panels": panels,
        },
        "overwrite": True,
        "folderId": 0,
    }, masked


def push(uid, title, description, tags, specs, ds_ref):
    payload, masked = build_dashboard(uid, title, description, tags, specs, ds_ref)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=AUTH, headers=HEADERS, json=payload)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print(f"ERREUR {title}: {r.text}")
        raise
    data = r.json()
    n_panels = len([p for p in payload["dashboard"]["panels"] if p.get("type") != "row"])
    print(f"OK   {title} -> {GRAFANA_URL}{data.get('url', '')} ({n_panels} panels actifs)")
    if masked:
        print(f"     masqués (no data): {masked}")
    return n_panels, masked


KAFKA_NATIVE_NOTE = """### Métriques Kafka / Strimzi natives indisponibles

Aucune métrique `kafka_*` ou `strimzi_*` n'est exposée dans Prometheus
(0 série trouvée) — le cluster Strimzi tourne en mode KRaft sans sidecar
JMX Exporter configuré. Les panels ci-dessous utilisent donc
**`swam_payments_total`** (exporteur métier custom) comme proxy réel du
débit du pipeline Kafka — pas de métrique fictive créée."""

KAFKA_LAG_NOTE = """### Consumer Lag indisponible

`kafka_consumergroup_lag` n'existe pas (même cause : pas de JMX Exporter
Kafka/Strimzi). Aucun proxy fiable n'existe pour le lag réel des consumer
groups — ce panel reste volontairement absent plutôt que d'afficher une
valeur inventée."""

NETWORK_IO_NOTE = """### Network I/O par namespace indisponible

Confirmé sur ce cluster : `container_network_receive_bytes_total` /
`..._transmit_bytes_total` n'ont que **2 séries au total**, toutes au
niveau du nœud (`id="/"`, interfaces `bridge`/`eth0`), sans label
`namespace` ni `pod`. Limitation connue de cAdvisor sur Minikube
mono-nœud — non corrigible côté dashboard."""


def specs_kafka():
    return [
        {"row": "Cluster Health (Kafka natif)"},
        {"text": "Note", "content": KAFKA_NATIVE_NOTE, "size": (24, 5)},

        {"row": "Pipeline Throughput (proxy swam_*)"},
        {"title": "Messages par stage", "type": "barchart", "size": (12, 8),
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}]},

        {"title": "Progression pipeline", "type": "timeseries", "size": (12, 8),
         "queries": [
             {"expr": 'swam_payments_total{stage="raw_encrypted"}', "legend": "raw_encrypted"},
             {"expr": 'swam_payments_total{stage="decrypted"}', "legend": "decrypted"},
             {"expr": 'swam_payments_total{stage="validated"}', "legend": "validated"},
             {"expr": 'swam_payments_total{stage="normalized"}', "legend": "normalized"},
             {"expr": 'swam_payments_total{stage="gold_enriched"}', "legend": "gold_enriched"},
             {"expr": 'swam_payments_total{stage="dead_letter"}', "legend": "dead_letter"},
         ]},

        {"title": "DLQ Growth", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}', "legend": "dead_letter"}],
         "fixed_color": "red"},

        {"title": "Gold Topic Growth", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": 'swam_payments_total{stage="gold_enriched"}', "legend": "gold_enriched"}],
         "fixed_color": "green"},

        {"title": "Producer Rate (raw_encrypted, msg/s)", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": 'rate(swam_payments_total{stage="raw_encrypted"}[1m])'}]},

        {"title": "Consumer Rate (gold_enriched, msg/s)", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": 'rate(swam_payments_total{stage="gold_enriched"}[1m])'}]},

        {"row": "Consumer Lag (Kafka natif)"},
        {"text": "Note", "content": KAFKA_LAG_NOTE, "size": (24, 5)},
    ]


def specs_k8s():
    return [
        {"row": "Cluster Health"},
        {"title": "Running Pods", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Running"})'}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Pending Pods", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Pending"})'}],
         "steps": [("green", None), ("orange", 1)]},
        {"title": "Failed Pods", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Failed"})'}],
         "steps": [("green", None), ("red", 1)]},
        {"title": "Nodes Ready", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(kube_node_status_condition{condition="Ready",status="true"})'}],
         "fixed_color": "blue", "color_mode": "background"},

        {"row": "CPU par namespace"},
        {"title": "CPU par namespace", "type": "timeseries", "size": (24, 8),
         "queries": [{"expr": 'sum(rate(container_cpu_usage_seconds_total'
                               '{namespace=~"kafka|flink|minio|kafka-connect|monitoring"}[5m])) by (namespace)',
                      "legend": "{{namespace}}"}],
         "unit": "short"},

        {"row": "Memoire par namespace"},
        {"title": "Memoire par namespace", "type": "timeseries", "size": (24, 8),
         "queries": [{"expr": 'sum(container_memory_working_set_bytes'
                               '{namespace=~"kafka|flink|minio|kafka-connect|monitoring"}) by (namespace)',
                      "legend": "{{namespace}}"}],
         "unit": "bytes"},

        {"row": "Pod Restarts"},
        {"title": "Top 10 Pod Restarts (1h)", "type": "barchart", "size": (24, 8),
         "queries": [{"expr": "topk(10, sum(increase(kube_pod_container_status_restarts_total[1h])) by (pod))",
                      "legend": "{{pod}}"}]},

        {"row": "Resource Pressure"},
        {"title": "CPU Requested par namespace", "type": "barchart", "size": (12, 8),
         "queries": [{"expr": 'sum(kube_pod_container_resource_requests{resource="cpu"}) by (namespace)',
                      "legend": "{{namespace}}"}]},
        {"title": "Memory Requested par namespace", "type": "barchart", "size": (12, 8),
         "queries": [{"expr": 'sum(kube_pod_container_resource_requests{resource="memory"}) by (namespace)',
                      "legend": "{{namespace}}"}],
         "unit": "bytes"},

        {"row": "Network I/O"},
        {"text": "Note", "content": NETWORK_IO_NOTE, "size": (24, 5)},
    ]


def specs_minio():
    return [
        {"row": "Storage Overview (swam_*)"},
        {"title": "Bronze Objects", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="bronze"}'}],
         "fixed_color": "orange", "color_mode": "background"},
        {"title": "Silver Objects", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="silver"}'}],
         "fixed_color": "blue", "color_mode": "background"},
        {"title": "Gold Objects", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="gold"}'}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Total Objects", "type": "stat", "size": (6, 4),
         "queries": [{"expr": "sum(swam_minio_objects)"}],
         "fixed_color": "purple", "color_mode": "background"},
        {"title": "Object Distribution", "type": "piechart", "size": (24, 8),
         "queries": [{"expr": "swam_minio_objects", "legend": "{{layer}}"}],
         "overrides": [("bronze", "orange"), ("silver", "blue"), ("gold", "green")]},

        {"row": "Storage Native MinIO"},
        {"title": "Storage Used", "type": "gauge", "size": (8, 8),
         "queries": [{"expr": "minio_cluster_usage_total_bytes"}],
         "unit": "bytes", "min": 0, "max": 10737418240},
        {"title": "Buckets", "type": "stat", "size": (8, 8),
         "queries": [{"expr": "minio_cluster_bucket_total"}],
         "fixed_color": "blue", "color_mode": "background"},
        {"title": "Objects Total (natif)", "type": "stat", "size": (8, 8),
         "queries": [{"expr": "minio_cluster_usage_object_total"}],
         "fixed_color": "purple", "color_mode": "background"},

        {"row": "API Performance"},
        {"title": "API Requests/sec", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": "rate(minio_s3_requests_total[1m])", "legend": "{{api}}"}],
         "axis_label": "req/s"},
        {"title": "API Errors/sec", "type": "timeseries", "size": (12, 8),
         "queries": [{"expr": "sum(rate(minio_s3_requests_errors_total[1m]))"}],
         "axis_label": "err/s", "fixed_color": "red"},
        {"title": "Erreurs MinIO Total", "type": "stat", "size": (12, 4),
         "queries": [{"expr": "sum(minio_s3_requests_errors_total) or vector(0)"}],
         "steps": [("green", None), ("orange", 1), ("red", 10)]},
        {"title": "API Latency p95 (TTFB)", "type": "timeseries", "size": (12, 4),
         "queries": [{"expr": "histogram_quantile(0.95, sum(rate("
                               "minio_s3_requests_ttfb_seconds_distribution[5m])) by (le))"}],
         "unit": "s"},
    ]


def specs_business_analytics():
    return [
        {"row": "KPIs principaux"},
        {"title": "Total Transactions (gold_transactions)", "type": "stat", "size": (6, 4),
         "queries": [{"expr": "swam_gold_transactions_total"}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Gold Enriched (Kafka)", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_payments_total{stage="gold_enriched"}'}],
         "fixed_color": "yellow", "color_mode": "background"},
        {"title": "DLQ Records", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red", "color_mode": "background"},
        {"title": "Rejection Rate %", "type": "gauge", "size": (6, 4),
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 10), ("red", 20)]},

        {"row": "Distribution Risk & Canal"},
        {"title": "Risk Score Distribution", "type": "piechart", "size": (12, 8),
         "queries": [{"expr": "swam_gold_risk_score_total", "legend": "{{risk}}"}],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},
        {"title": "Payment Channel Distribution", "type": "piechart", "size": (12, 8),
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},

        {"row": "Risk Stats détaillées"},
        {"title": "Risk HIGH %", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(swam_gold_risk_score_total{risk="HIGH"}) / '
                               'sum(swam_gold_risk_score_total) * 100'}],
         "unit": "percent", "fixed_color": "red", "color_mode": "background"},
        {"title": "Risk MEDIUM %", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(swam_gold_risk_score_total{risk="MEDIUM"}) / '
                               'sum(swam_gold_risk_score_total) * 100'}],
         "unit": "percent", "fixed_color": "orange", "color_mode": "background"},
        {"title": "Risk LOW %", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(swam_gold_risk_score_total{risk="LOW"}) / '
                               'sum(swam_gold_risk_score_total) * 100'}],
         "unit": "percent", "fixed_color": "green", "color_mode": "background"},
        {"title": "Risk Score Trend", "type": "timeseries", "size": (12, 4),
         "queries": [
             {"expr": 'swam_gold_risk_score_total{risk="HIGH"}', "legend": "HIGH"},
             {"expr": 'swam_gold_risk_score_total{risk="MEDIUM"}', "legend": "MEDIUM"},
             {"expr": 'swam_gold_risk_score_total{risk="LOW"}', "legend": "LOW"},
         ],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},

        {"row": "Pipeline Throughput"},
        {"title": "Pipeline Stage Volumes", "type": "barchart", "size": (24, 8),
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}]},
        {"title": "Payment Channel Trend", "type": "timeseries", "size": (24, 8),
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},
    ]


def specs_business_quality():
    return [
        {"row": "Pipeline Health Stats"},
        {"title": "Valid Transactions", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_payments_total{stage="validated"}'}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Invalid Transactions", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red", "color_mode": "background"},
        {"title": "Validation %", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(swam_payments_total{stage="validated"}) / '
                               'sum(swam_payments_total{stage="decrypted"}) * 100'}],
         "unit": "percent", "fixed_color": "blue", "color_mode": "background"},
        {"title": "Gold Conversion %", "type": "stat", "size": (6, 4),
         "queries": [{"expr": 'sum(swam_payments_total{stage="gold_enriched"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"}) * 100'}],
         "unit": "percent", "fixed_color": "yellow", "color_mode": "background"},

        {"row": "Rejection Rate & Data Quality Score"},
        {"title": "Rejection Rate", "type": "gauge", "size": (12, 8),
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 10), ("red", 20)]},
        {"title": "Data Quality Score", "type": "gauge", "size": (12, 8),
         "queries": [{"expr": '(sum(swam_payments_total{stage="validated"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("red", None), ("orange", 70), ("green", 85)]},

        {"row": "DLQ Activity"},
        {"title": "DLQ Trend", "type": "timeseries", "size": (24, 8),
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red"},

        {"row": "Pipeline Funnel"},
        {"title": "Pipeline Stage Funnel", "type": "barchart", "size": (24, 8),
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}],
         "overrides": [("dead_letter", "red")]},
    ]


def specs_exec_overview():
    return [
        {"row": "Top KPIs"},
        {"title": "Payments Processed", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(swam_payments_total{stage="raw_encrypted"})'}],
         "fixed_color": "blue", "color_mode": "background"},
        {"title": "Gold Records", "type": "stat", "size": (4, 4),
         "queries": [{"expr": "swam_gold_transactions_total"}],
         "fixed_color": "yellow", "color_mode": "background"},
        {"title": "DLQ Records", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(swam_payments_total{stage="dead_letter"})'}],
         "fixed_color": "red", "color_mode": "background"},
        {"title": "Rejection Rate", "type": "gauge", "size": (4, 4),
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 10), ("red", 20)]},
        {"title": "Flink Jobs", "type": "stat", "size": (4, 4),
         "queries": [{"expr": "sum(flink_jobmanager_numRunningJobs)"}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "MinIO Objects", "type": "stat", "size": (4, 4),
         "queries": [{"expr": "sum(swam_minio_objects)"}],
         "fixed_color": "purple", "color_mode": "background"},

        {"row": "Pipeline Flow Summary"},
        {"title": "Pipeline Stage Flow", "type": "barchart", "size": (24, 8),
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}],
         "overrides": [("dead_letter", "red")]},

        {"row": "Business Distributions"},
        {"title": "Risk Distribution", "type": "piechart", "size": (12, 6),
         "queries": [{"expr": "swam_gold_risk_score_total", "legend": "{{risk}}"}],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},
        {"title": "Channel Distribution", "type": "piechart", "size": (12, 6),
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},

        {"row": "System Health Indicators"},
        {"title": "TaskManagers", "type": "stat", "size": (4, 4),
         "queries": [{"expr": "sum(flink_jobmanager_numRegisteredTaskManagers)"}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "MinIO Buckets", "type": "stat", "size": (4, 4),
         "queries": [{"expr": "minio_cluster_bucket_total"}],
         "fixed_color": "purple", "color_mode": "background"},
        {"title": "K8s Pods Running", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'sum(kube_pod_status_phase{phase="Running"})'}],
         "fixed_color": "blue", "color_mode": "background"},
        {"title": "Bronze Objects", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="bronze"}'}],
         "fixed_color": "orange", "color_mode": "background"},
        {"title": "Silver Objects", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="silver"}'}],
         "fixed_color": "blue", "color_mode": "background"},
        {"title": "Gold Objects", "type": "stat", "size": (4, 4),
         "queries": [{"expr": 'swam_minio_objects{layer="gold"}'}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Kafka Topics (indisponible)", "_skip_reason":
         "Aucune metrique kafka_topic_* native -- non fabriquee, panel omis."},
    ]


def specs_data_quality():
    return [
        {"row": "Validation"},
        {"title": "Validated Transactions", "type": "stat", "size": (8, 4),
         "queries": [{"expr": 'swam_payments_total{stage="validated"}'}],
         "fixed_color": "green", "color_mode": "background"},
        {"title": "Rejected (DLQ)", "type": "stat", "size": (8, 4),
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red", "color_mode": "background"},
        {"title": "Rejection Rate %", "type": "gauge", "size": (8, 4),
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 10), ("red", 20)]},

        {"row": "Volumes"},
        {"title": "Pipeline Stage Volumes", "type": "barchart", "size": (12, 8),
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}]},
        {"title": "MinIO Medallion Object Counts", "type": "barchart", "size": (12, 8),
         "queries": [{"expr": "swam_minio_objects", "legend": "{{layer}}"}],
         "overrides": [("bronze", "orange"), ("silver", "blue"), ("gold", "green")]},

        {"row": "Tendance"},
        {"title": "Stage Throughput Over Time", "type": "timeseries", "size": (24, 8),
         "queries": [
             {"expr": 'swam_payments_total{stage="raw_encrypted"}', "legend": "Encrypted"},
             {"expr": 'swam_payments_total{stage="validated"}', "legend": "Validated"},
             {"expr": 'swam_payments_total{stage="gold_enriched"}', "legend": "Gold Enriched"},
             {"expr": 'swam_payments_total{stage="dead_letter"}', "legend": "Dead Letter"},
         ]},
    ]


DASHBOARDS = [
    ("swam-kafka-monitoring", "SWAM - Kafka Monitoring",
     "Monitoring du pipeline Kafka, inspire de 'Kafka Metrics Dashboard' (24565), "
     "adapte : kafka_*/strimzi_* absents sur ce cluster KRaft sans JMX exporter -- "
     "swam_payments_total utilise comme proxy reel du debit.",
     ["swam", "kafka", "pipeline", "poc"], specs_kafka),
    ("swam-k8s-infrastructure", "SWAM - Kubernetes Infrastructure",
     "Monitoring infrastructure K8s, inspire de 'K8S Dashboard' (15661), adapte aux "
     "metriques cAdvisor/kube-state-metrics reellement exposees sur ce cluster mono-noeud.",
     ["swam", "kubernetes", "infra", "poc"], specs_k8s),
    ("swam-minio-monitoring", "SWAM - MinIO Monitoring",
     "Monitoring MinIO (le dashboard officiel 13502 est introuvable/retire de "
     "grafana.com -- construit directement depuis les 77 metriques minio_* reelles).",
     ["swam", "minio", "storage", "poc"], specs_minio),
    ("swam-business-analytics", "SWAM - Business Analytics",
     "KPIs metier (gold_transactions, risk score, canal de paiement) -- design "
     "from scratch inspire des patterns Grafana Play.",
     ["swam", "business", "poc"], specs_business_analytics),
    ("swam-business-quality", "SWAM - Business & Data Quality",
     "Qualite des donnees du pipeline (validation, rejet, conversion gold) -- design "
     "from scratch inspire des patterns Grafana Play.",
     ["swam", "business", "quality", "poc"], specs_business_quality),
    ("swam-exec-overview", "SWAM - Executive Overview",
     "Vue C-level en un coup d'oeil -- design from scratch, manager-friendly.",
     ["swam", "executive", "poc"], specs_exec_overview),
    ("swam-data-quality", "SWAM - Data Quality",
     "Qualite des donnees du pipeline -- volumes et tendances par etape.",
     ["swam", "quality", "poc"], specs_data_quality),
]


def main():
    ds_uid = ensure_datasource()
    ds_ref = {"type": "prometheus", "uid": ds_uid}

    print("\n--- Upgrade des 7 dashboards (Flink deja fait) ---\n")
    report = []
    for uid, title, desc, tags, spec_fn in DASHBOARDS:
        specs = [s for s in spec_fn() if "_skip_reason" not in s]
        n_panels, masked = push(uid, title, desc, tags, specs, ds_ref)
        report.append((title, n_panels, masked))

    print("\n--- Resume ---")
    for title, n_panels, masked in report:
        print(f"{title:40} panels actifs={n_panels:3}  masques={masked or 'aucun'}")


if __name__ == "__main__":
    main()
