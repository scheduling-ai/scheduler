output "kubeconfig_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --zone ${var.zone} --project ${var.project_id}"
}
