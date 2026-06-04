resource "helm_release" "strimzi_operator" {
  name             = "strimzi-kafka-operator"
  repository       = "https://strimzi.io/charts/"
  chart            = "strimzi-kafka-operator"
  version          = var.strimzi_version
  namespace        = var.ingestion_namespace
  create_namespace = false
  depends_on       = [kubernetes_namespace.ingestion]

  set {
    name  = "resources.requests.memory"
    value = "256Mi"
  }
  set {
    name  = "resources.requests.cpu"
    value = "100m"
  }

  cleanup_on_fail = true
  timeout         = 600
  wait            = true
}

resource "null_resource" "wait_strimzi_crds" {
  depends_on = [helm_release.strimzi_operator]

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-SCRIPT
      set -e
      echo "[CRD wait] Attente des CRDs Strimzi..."
      for crd in kafkas.kafka.strimzi.io kafkatopics.kafka.strimzi.io kafkausers.kafka.strimzi.io; do
        echo -n "  $crd ..."
        for i in $(seq 1 60); do
          kubectl get crd "$crd" &>/dev/null && break
          echo -n "."
          sleep 3
        done
        kubectl get crd "$crd" &>/dev/null || { echo " TIMEOUT"; exit 1; }
        kubectl wait crd "$crd" --for=condition=Established --timeout=60s
        echo " OK"
      done
      echo "[CRD wait] CRDs Strimzi prets."
    SCRIPT
  }
}
