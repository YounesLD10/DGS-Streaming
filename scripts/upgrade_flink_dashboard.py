"""
Replace SWAM - Flink Monitoring with an enterprise-grade dashboard inspired by
the official Grafana dashboard 14911 ("Apache Flink 2021 Dashboard for Job /
Task Manager"), adapted to the metrics actually exported by this project's
Flink JobManager/TaskManager Prometheus reporters.

Same UID/title as before (swam-flink-monitoring) -> overwrite, not a new
dashboard. The 8 existing SWAM dashboards stay at 8.

Deviation from the official 14911 GC row: this JVM runs the Parallel
collector (Copy + MarkSweepCompact), not G1, so the GC panel uses
flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond instead of
the G1-specific series referenced by 14911.
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

UID = "swam-flink-monitoring"
TITLE = "SWAM - Flink Monitoring"


def row_panel(title, pos):
    return {"type": "row", "title": title, "gridPos": pos, "collapsed": False, "panels": []}


SPECS = [
    {"title": "__row__ Overview", "row": "Overview"},

    {"title": "Running Jobs", "type": "stat", "size": (4, 4),
     "queries": [{"expr": "sum(flink_jobmanager_numRunningJobs)"}],
     "steps": [("red", None), ("green", 1)]},

    {"title": "TaskManagers", "type": "stat", "size": (4, 4),
     "queries": [{"expr": "sum(flink_jobmanager_numRegisteredTaskManagers)"}],
     "steps": [("red", None), ("green", 1)]},

    {"title": "Total Slots", "type": "stat", "size": (4, 4),
     "queries": [{"expr": "sum(flink_jobmanager_taskSlotsTotal)"}],
     "fixed_color": "blue", "color_mode": "background"},

    {"title": "Available Slots", "type": "stat", "size": (4, 4),
     "queries": [{"expr": "sum(flink_jobmanager_taskSlotsAvailable)"}],
     "fixed_color": "green", "color_mode": "background"},

    {"title": "Used Slots", "type": "stat", "size": (4, 4),
     "queries": [{"expr": "sum(flink_jobmanager_taskSlotsTotal) - "
                           "sum(flink_jobmanager_taskSlotsAvailable)"}],
     "fixed_color": "orange", "color_mode": "background"},

    {"title": "__row__ Checkpoints", "row": "Checkpoints"},

    {"title": "Last Checkpoint Duration", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "flink_jobmanager_job_lastCheckpointDuration", "legend": "{{job_name}}"}],
     "unit": "ms"},

    {"title": "Last Checkpoint Size", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "flink_jobmanager_job_lastCheckpointSize", "legend": "{{job_name}}"}],
     "unit": "bytes"},

    {"title": "Completed Checkpoints", "type": "stat", "size": (12, 4),
     "queries": [{"expr": "sum(flink_jobmanager_job_numberOfCompletedCheckpoints)"}],
     "fixed_color": "green", "color_mode": "background"},

    {"title": "Failed Checkpoints", "type": "stat", "size": (12, 4),
     "queries": [{"expr": "sum(flink_jobmanager_job_numberOfFailedCheckpoints) or vector(0)"}],
     "steps": [("green", None), ("red", 1)]},

    {"title": "__row__ Throughput", "row": "Throughput"},

    {"title": "Records In/sec", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "sum(rate(flink_taskmanager_job_task_operator_numRecordsIn[1m])) by (job_name)",
                  "legend": "{{job_name}}"}],
     "unit": "ops"},

    {"title": "Records Out/sec", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "sum(rate(flink_taskmanager_job_task_operator_numRecordsOut[1m])) by (job_name)",
                  "legend": "{{job_name}}"}],
     "unit": "ops"},

    {"title": "__row__ Backpressure & Performance", "row": "Backpressure & Performance"},

    {"title": "Backpressure", "type": "timeseries", "size": (8, 8),
     "queries": [{"expr": "avg(flink_taskmanager_job_task_isBackPressured) by (task_name)",
                  "legend": "{{task_name}}"}]},

    {"title": "Busy Time ms/s", "type": "timeseries", "size": (8, 8),
     "queries": [{"expr": "avg(flink_taskmanager_job_task_busyTimeMsPerSecond) by (task_name)",
                  "legend": "{{task_name}}"}],
     "unit": "ms"},

    {"title": "Idle Time ms/s", "type": "timeseries", "size": (8, 8),
     "queries": [{"expr": "avg(flink_taskmanager_job_task_idleTimeMsPerSecond) by (task_name)",
                  "legend": "{{task_name}}"}],
     "unit": "ms"},

    {"title": "__row__ JVM (TaskManagers)", "row": "JVM (TaskManagers)"},

    {"title": "Heap Used", "type": "timeseries", "size": (8, 8),
     "queries": [{"expr": "flink_taskmanager_Status_JVM_Memory_Heap_Used", "legend": "{{tm_id}}"}],
     "unit": "bytes"},

    {"title": "Heap %", "type": "gauge", "size": (8, 8),
     "queries": [{"expr": "flink_taskmanager_Status_JVM_Memory_Heap_Used / "
                           "flink_taskmanager_Status_JVM_Memory_Heap_Max * 100",
                  "legend": "{{tm_id}}"}],
     "unit": "percent", "min": 0, "max": 100,
     "steps": [("green", None), ("orange", 75), ("red", 90)]},

    {"title": "CPU Load", "type": "timeseries", "size": (8, 8),
     "queries": [{"expr": "flink_taskmanager_Status_JVM_CPU_Load * 100", "legend": "{{tm_id}}"}],
     "unit": "percent"},

    {"title": "GC Time (Copy + MarkSweepCompact)", "type": "timeseries", "size": (24, 8),
     "queries": [{"expr": "flink_taskmanager_Status_JVM_GarbageCollector_All_TimeMsPerSecond",
                  "legend": "{{tm_id}}"}],
     "unit": "ms"},

    {"title": "__row__ JVM (JobManager)", "row": "JVM (JobManager)"},

    {"title": "JM Heap Used", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "flink_jobmanager_Status_JVM_Memory_Heap_Used"}],
     "unit": "bytes"},

    {"title": "JM CPU Load", "type": "timeseries", "size": (12, 8),
     "queries": [{"expr": "flink_jobmanager_Status_JVM_CPU_Load * 100"}],
     "unit": "percent"},
]


def build_dashboard(ds_ref):
    layout = GridLayout()
    panels = []
    for spec in SPECS:
        if "row" in spec:
            pos = layout.place(24, 1)
            panels.append(row_panel(spec["row"], pos))
            continue
        w, h = spec.get("size", DEFAULT_SIZE[spec["type"]])
        panels.append(build_panel(spec, ds_ref, layout.place(w, h)))
    return {
        "dashboard": {
            "uid": UID,
            "title": TITLE,
            "description": "Enterprise-grade Flink monitoring inspired by the official "
                            "dashboard 14911, adapted to PyFlink/Prometheus metrics actually "
                            "exported in this project.",
            "tags": ["swam", "flink", "advanced", "poc"],
            "timezone": "browser",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-1h", "to": "now"},
            "panels": panels,
        },
        "overwrite": True,
        "folderId": 0,
    }


def main():
    ds_uid = ensure_datasource()
    ds_ref = {"type": "prometheus", "uid": ds_uid}
    payload = build_dashboard(ds_ref)
    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=AUTH, headers=HEADERS, json=payload)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        print(r.text)
        raise
    data = r.json()
    print(f"OK   {TITLE} -> {GRAFANA_URL}{data.get('url', '')}")


if __name__ == "__main__":
    main()
