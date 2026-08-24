# ============================================================================
# kafka-connect namespace: PostgreSQL Data Mart + gold-flattener
# ============================================================================
# Python runtime versions (all pinned to var.python_runtime_image =
# python:3.11-slim for kafka-python compatibility — kafka-python's vendored
# `six` import breaks on 3.12):
#   - gold-flattener: python:3.11-slim (Terraform-managed below)
#   - gold-sink:      python:3.11-slim (pre-existing Deployment, not
#                     Terraform-managed — keep in sync with the variable
#                     above by hand if either is ever bumped)
#   - swam_exporter (hps_exporter.py): not containerized — runs on the host
#                     Python interpreter via port-forward, not subject to
#                     in-cluster image pinning
#   - Flink JobManager/TaskManager: apache/flink:1.19.1-scala_2.12-java11
#                     (see flink.tf) — bundles its own PyFlink/Python env,
#                     unrelated to this project's kafka-python version
#                     constraint, documented here for completeness only
# ============================================================================
#
# The "kafka-connect" namespace and the "postgres-datamart" / "postgres-hps"
# PostgreSQL 15 deployments already exist in the cluster (created outside
# Terraform). This file does NOT recreate those Deployments/Services -
# doing so would collide with the running resources. Instead it tracks the
# schema-init SQL as ConfigMaps and applies it idempotently via psql against
# the existing pods:
#
#   postgres-datamart / database "datamart"
#       -> Star schema data mart: dim_risk, dim_canal, dim_banque, dim_date,
#          fact_transactions, v_risk_summary view. RETIRED/FROZEN as of the
#          gold_transactions migration below — gold-sink (the Python bridge
#          that fed this schema) is paused (0 replicas), not deleted. Tables
#          are left in place for reference/rollback but no longer receive data.
#       -> gold_transactions: single flat table, the current production
#          target of gold_flattener.py -> Kafka Connect JDBC sink connector
#          "gold-transactions-sink" (payments.gold.flat -> gold_transactions).
#
#   postgres-hps / database "hps_db"
#       -> source.transactions (table "public.transactions"): activates the
#          pre-registered Debezium CDC source connector "debezium-hps-source"
#          (table.include.list=public.transactions, topic.prefix=hps).
#
# Both SQL scripts are idempotent (CREATE TABLE IF NOT EXISTS, ON CONFLICT
# DO NOTHING, CREATE OR REPLACE ...) so re-applying is safe.
# ============================================================================

resource "kubernetes_config_map" "datamart_schema_sql" {
  metadata {
    name      = "datamart-schema-sql"
    namespace = var.kafka_connect_namespace
  }

  data = {
    "schema.sql" = file("${path.module}/../sql/datamart_schema.sql")
  }
}

resource "kubernetes_config_map" "gold_transactions_schema_sql" {
  metadata {
    name      = "gold-transactions-schema-sql"
    namespace = var.kafka_connect_namespace
  }

  data = {
    "schema.sql" = file("${path.module}/../sql/gold_transactions_schema.sql")
  }
}

resource "kubernetes_config_map" "source_transactions_schema_sql" {
  metadata {
    name      = "source-transactions-schema-sql"
    namespace = var.kafka_connect_namespace
  }

  data = {
    "schema.sql" = file("${path.module}/../sql/source_transactions_schema.sql")
  }
}

# Apply the star schema to postgres-datamart/datamart.
resource "null_resource" "apply_datamart_schema" {
  triggers = {
    sql_md5 = filemd5("${path.module}/../sql/datamart_schema.sql")
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      POD=$(minikube kubectl -- get pod -n ${var.kafka_connect_namespace} -l app=postgres-datamart -o jsonpath='{.items[0].metadata.name}')
      minikube kubectl -- exec -i -n ${var.kafka_connect_namespace} "$POD" -- psql -U hps -d datamart < ${path.module}/../sql/datamart_schema.sql
    EOT
  }

  depends_on = [kubernetes_config_map.datamart_schema_sql]
}

# Apply the flat gold_transactions table to postgres-datamart/datamart.
resource "null_resource" "apply_gold_transactions_schema" {
  triggers = {
    sql_md5 = filemd5("${path.module}/../sql/gold_transactions_schema.sql")
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      POD=$(minikube kubectl -- get pod -n ${var.kafka_connect_namespace} -l app=postgres-datamart -o jsonpath='{.items[0].metadata.name}')
      minikube kubectl -- exec -i -n ${var.kafka_connect_namespace} "$POD" -- psql -U hps -d datamart < ${path.module}/../sql/gold_transactions_schema.sql
    EOT
  }

  depends_on = [kubernetes_config_map.gold_transactions_schema_sql]
}

# Apply source.transactions to postgres-hps/hps_db (activates Debezium CDC source).
resource "null_resource" "apply_source_transactions_schema" {
  triggers = {
    sql_md5 = filemd5("${path.module}/../sql/source_transactions_schema.sql")
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      POD=$(minikube kubectl -- get pod -n ${var.kafka_connect_namespace} -l app=postgres-hps -o jsonpath='{.items[0].metadata.name}')
      minikube kubectl -- exec -i -n ${var.kafka_connect_namespace} "$POD" -- psql -U hps -d hps_db < ${path.module}/../sql/source_transactions_schema.sql
    EOT
  }

  depends_on = [kubernetes_config_map.source_transactions_schema_sql]
}

# ── gold-flattener ────────────────────────────────────────────────────────────
# payments.gold -> payments.gold.flat bridge (permanent pipeline component,
# feeds the gold-transactions-sink Kafka Connect connector). Brought under
# Terraform after being deployed ad hoc and lost once on pod recreation.
#
# The script ConfigMap is a native kubernetes_config_map (matching the SQL
# ConfigMaps above). The Deployment itself uses null_resource + kubectl apply
# rather than kubernetes_deployment: no Deployment in this project is managed
# as a native Terraform resource (Kafka/Flink workloads in kafka.tf/flink.tf
# follow the same null_resource + kubectl-apply convention), since the
# kafka-connect namespace's underlying Deployments (postgres-datamart,
# postgres-hps, gold-sink) were created outside Terraform and this keeps
# gold-flattener consistent with that established pattern.
resource "kubernetes_config_map" "gold_flattener_script" {
  metadata {
    name      = "gold-flattener-script"
    namespace = var.kafka_connect_namespace
  }

  data = {
    "gold_flattener.py" = file("${path.module}/../scripts/gold_flattener.py")
  }
}

resource "null_resource" "gold_flattener_deployment" {
  triggers = {
    script_md5 = filemd5("${path.module}/../scripts/gold_flattener.py")
    image      = var.python_runtime_image
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      minikube kubectl -- apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gold-flattener
  namespace: ${var.kafka_connect_namespace}
  labels:
    app: gold-flattener
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gold-flattener
  template:
    metadata:
      labels:
        app: gold-flattener
    spec:
      containers:
        - name: gold-flattener
          image: ${var.python_runtime_image}
          command: ["sh", "-c"]
          args:
            - pip install --no-cache-dir kafka-python==2.0.2 >/dev/null 2>&1 && python /app/gold_flattener.py
          volumeMounts:
            - name: script
              mountPath: /app
      volumes:
        - name: script
          configMap:
            name: gold-flattener-script
YAML
      minikube kubectl -- rollout restart deployment/gold-flattener -n ${var.kafka_connect_namespace}
      minikube kubectl -- rollout status deployment/gold-flattener -n ${var.kafka_connect_namespace} --timeout=120s
    EOT
  }

  depends_on = [kubernetes_config_map.gold_flattener_script]
}
