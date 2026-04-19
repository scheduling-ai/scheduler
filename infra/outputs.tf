output "kubeconfig_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --zone ${var.zone} --project ${var.project_id}"
}

output "image_repo" {
  description = "Artifact Registry URL used for scheduler container images."
  value       = "${google_artifact_registry_repository.images.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "cluster_endpoint" {
  description = "Public API endpoint of the GKE cluster (used by the in-cluster kubeconfig Secret)."
  value       = google_container_cluster.this.endpoint
  sensitive   = true
}

output "cluster_ca_certificate" {
  description = "Base64-encoded cluster CA cert."
  value       = google_container_cluster.this.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "cluster_name" {
  value = google_container_cluster.this.name
}

output "zone" {
  value = var.zone
}

output "project_id" {
  value = var.project_id
}
