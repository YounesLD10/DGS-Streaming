# Namespaces
resource "kubernetes_namespace" "ingestion" {
  metadata {
    name   = var.ingestion_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}

resource "kubernetes_namespace" "traitement" {
  metadata {
    name   = var.traitement_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}

resource "kubernetes_namespace" "stockage" {
  metadata {
    name   = var.stockage_namespace
    labels = { "app.kubernetes.io/part-of" = "poc-pipeline" }
  }
}
