# ── Flink Session Cluster ──────────────────────────────────────────────────────
# Flink 1.19 uses config.yaml (hierarchical YAML), not flink-conf.yaml.
# config-parser-utils.sh writes to config.yaml at startup, so a direct
# ConfigMap subPath mount (read-only) causes a crash. Fix: initContainer
# copies the original conf dir to a writable emptyDir, then overlays our
# config.yaml. JVM overhead and metaspace defaults are also reduced to fit
# within the 512m process budget on a 3072m Minikube node.
resource "null_resource" "flink_cluster" {
  depends_on = [kubernetes_namespace.flink]

  triggers = {
    namespace  = kubernetes_namespace.flink.metadata[0].name
    config_ver = "v6"
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      echo "[flink] Deploying Flink 1.19 session cluster..."

      minikube kubectl -- apply -f - <<'YAML'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink
  namespace: flink
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-config
  namespace: flink
data:
  config.yaml: |
    jobmanager:
      rpc:
        address: flink-jobmanager
        port: 6123
      memory:
        process:
          size: 512m
        jvm-metaspace:
          size: 96m
        jvm-overhead:
          min: 32m
          max: 128m
          fraction: 0.1
      execution:
        failover-strategy: region
      bind-host: 0.0.0.0
    taskmanager:
      memory:
        process:
          size: 512m
        jvm-metaspace:
          size: 96m
        jvm-overhead:
          min: 32m
          max: 128m
          fraction: 0.1
        framework:
          heap:
            size: 64m
          off-heap:
            size: 64m
      numberOfTaskSlots: 2
      bind-host: 0.0.0.0
    parallelism:
      default: 2
    rest:
      port: 8081
      address: 0.0.0.0
      bind-address: 0.0.0.0
    env:
      java:
        opts:
          all: --add-exports=java.base/sun.net.util=ALL-UNNAMED --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
  namespace: flink
  labels:
    app: flink
    component: jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      serviceAccountName: flink
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
        - name: flink-conf-dir
          emptyDir: {}
      initContainers:
        - name: copy-config
          image: apache/flink:1.19.1-scala_2.12-java11
          command: ["sh", "-c", "cp -r /opt/flink/conf/. /tmp/conf/ && cp /tmp/configmap/config.yaml /tmp/conf/config.yaml"]
          volumeMounts:
            - name: flink-config-volume
              mountPath: /tmp/configmap
            - name: flink-conf-dir
              mountPath: /tmp/conf
      containers:
        - name: jobmanager
          image: apache/flink:1.19.1-scala_2.12-java11
          args: ["jobmanager"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob
            - containerPort: 8081
              name: rest
          volumeMounts:
            - name: flink-conf-dir
              mountPath: /opt/flink/conf
          resources:
            requests:
              memory: 256Mi
              cpu: 100m
            limits:
              memory: 768Mi
              cpu: 500m
          readinessProbe:
            httpGet:
              path: /overview
              port: 8081
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 12
---
apiVersion: v1
kind: Service
metadata:
  name: flink-jobmanager
  namespace: flink
  labels:
    app: flink
    component: jobmanager
spec:
  selector:
    app: flink
    component: jobmanager
  ports:
    - name: rpc
      port: 6123
      targetPort: 6123
    - name: blob
      port: 6124
      targetPort: 6124
    - name: rest
      port: 8081
      targetPort: 8081
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
  namespace: flink
  labels:
    app: flink
    component: taskmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      serviceAccountName: flink
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config
        - name: flink-conf-dir
          emptyDir: {}
      initContainers:
        - name: copy-config
          image: apache/flink:1.19.1-scala_2.12-java11
          command: ["sh", "-c", "cp -r /opt/flink/conf/. /tmp/conf/ && cp /tmp/configmap/config.yaml /tmp/conf/config.yaml"]
          volumeMounts:
            - name: flink-config-volume
              mountPath: /tmp/configmap
            - name: flink-conf-dir
              mountPath: /tmp/conf
      containers:
        - name: taskmanager
          image: apache/flink:1.19.1-scala_2.12-java11
          args: ["taskmanager"]
          ports:
            - containerPort: 6122
              name: rpc
          volumeMounts:
            - name: flink-conf-dir
              mountPath: /opt/flink/conf
          resources:
            requests:
              memory: 256Mi
              cpu: 100m
            limits:
              memory: 768Mi
              cpu: 500m
YAML

      echo "[flink] Waiting for Flink pods to be ready (up to 10 min)..."
      minikube kubectl -- wait deployment/flink-jobmanager  -n flink --for=condition=Available --timeout=600s
      minikube kubectl -- wait deployment/flink-taskmanager -n flink --for=condition=Available --timeout=600s
      echo "[flink] Flink session cluster ready."
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      minikube kubectl -- delete deployment flink-jobmanager flink-taskmanager -n flink --ignore-not-found || true
      minikube kubectl -- delete service flink-jobmanager -n flink --ignore-not-found || true
      minikube kubectl -- delete configmap flink-config -n flink --ignore-not-found || true
      minikube kubectl -- delete serviceaccount flink -n flink --ignore-not-found || true
    EOT
  }
}
