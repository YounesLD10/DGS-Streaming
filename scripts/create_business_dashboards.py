"""
Create 2 new Grafana dashboards driven by the hps_exporter.py business
metrics (swam_payments_total, swam_minio_objects, swam_gold_transactions_total,
swam_gold_risk_score_total, swam_gold_payment_channel_total — the live
gold_transactions path; swam_datamart_total/swam_risk_score_total/
swam_payment_channel_total still exist but reflect the retired/frozen
Star Schema and no longer grow):

  1. SWAM - Business Analytics
  2. SWAM - Data Quality

These are ADDED alongside the existing dashboards created by
create_dashboards.py — nothing there is modified or overwritten.
Tagged ["hps", "business", "poc"], refresh=30s, time range=last 1h.
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

TAGS = ["swam", "business", "poc"]


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
            "tags": TAGS,
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
# Dashboard 1: Business Analytics
# ---------------------------------------------------------------------------

def dashboard_business_analytics():
    return [
        {"title": "Total Transactions (gold_transactions, live)", "type": "stat",
         "queries": [{"expr": "swam_gold_transactions_total"}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Gold Enriched (Kafka)", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="gold_enriched"}'}],
         "fixed_color": "blue", "color_mode": "background"},

        {"title": "Risk Score Distribution", "type": "piechart",
         "queries": [{"expr": "swam_gold_risk_score_total", "legend": "{{risk}}"}],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},

        {"title": "Payment Channel Distribution", "type": "piechart",
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},

        {"title": "Risk Score Trend", "type": "timeseries",
         "queries": [
             {"expr": 'swam_gold_risk_score_total{risk="HIGH"}', "legend": "HIGH"},
             {"expr": 'swam_gold_risk_score_total{risk="MEDIUM"}', "legend": "MEDIUM"},
             {"expr": 'swam_gold_risk_score_total{risk="LOW"}', "legend": "LOW"},
         ],
         "overrides": [("HIGH", "red"), ("MEDIUM", "orange"), ("LOW", "green")]},

        {"title": "Payment Channel Trend", "type": "timeseries",
         "queries": [{"expr": "swam_gold_payment_channel_total", "legend": "{{channel}}"}]},
    ]


# ---------------------------------------------------------------------------
# Dashboard 2: Data Quality
# ---------------------------------------------------------------------------

def dashboard_data_quality():
    return [
        {"title": "Validated Transactions", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="validated"}'}],
         "fixed_color": "green", "color_mode": "background"},

        {"title": "Rejected (DLQ)", "type": "stat",
         "queries": [{"expr": 'swam_payments_total{stage="dead_letter"}'}],
         "fixed_color": "red", "color_mode": "background"},

        {"title": "Rejection Rate %", "type": "gauge",
         "queries": [{"expr": '(sum(swam_payments_total{stage="dead_letter"}) / '
                               'sum(swam_payments_total{stage="raw_encrypted"})) * 100'}],
         "unit": "percent", "min": 0, "max": 100,
         "steps": [("green", None), ("orange", 20), ("red", 30)]},

        {"title": "Pipeline Stage Volumes", "type": "barchart",
         "queries": [{"expr": "swam_payments_total", "legend": "{{stage}}"}]},

        {"title": "MinIO Medallion Object Counts", "type": "barchart",
         "queries": [{"expr": "swam_minio_objects", "legend": "{{layer}}"}],
         "overrides": [("bronze", "orange"), ("silver", "blue"), ("gold", "green")]},

        {"title": "Stage Throughput Over Time", "type": "timeseries",
         "queries": [
             {"expr": 'swam_payments_total{stage="raw_encrypted"}', "legend": "Encrypted"},
             {"expr": 'swam_payments_total{stage="validated"}', "legend": "Validated"},
             {"expr": 'swam_payments_total{stage="gold_enriched"}', "legend": "Gold Enriched"},
             {"expr": 'swam_payments_total{stage="dead_letter"}', "legend": "Dead Letter"},
         ],
         "overrides": [
             ("Encrypted", "blue"), ("Validated", "#a3e635"),
             ("Gold Enriched", "green"), ("Dead Letter", "red"),
         ]},
    ]


DASHBOARDS = [
    ("swam-business-analytics", "SWAM - Business Analytics", dashboard_business_analytics),
    ("swam-data-quality", "SWAM - Data Quality", dashboard_data_quality),
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

    print("\n--- Summary ---")
    for title, url, status in results:
        print(f"{status:12s} {title:35s} {url or ''}")

    if any(status != "OK" for _, _, status in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
