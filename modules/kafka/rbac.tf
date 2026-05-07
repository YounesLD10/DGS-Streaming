resource "kubernetes_role_v1" "flink_role" {
  metadata {
    name      = "flink-role"
    namespace = "flink"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services"]
    verbs      = ["get", "list", "watch", "create", "delete"]
  }
}

resource "kubernetes_role_binding_v1" "flink_role_binding" {
  metadata {
    name      = "flink-role-binding"
    namespace = "flink"
  }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role_v1.flink_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = "default"
    namespace = "flink"
  }
}