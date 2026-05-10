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

# --- Observed cluster ---

output "observed_cluster_name" {
  value = google_container_cluster.observed.name
}

output "observed_cluster_endpoint" {
  description = "Public API endpoint of the observed cluster (used by scripts/setup-observed.sh to mint a kubeconfig)."
  value       = google_container_cluster.observed.endpoint
  sensitive   = true
}

output "observed_cluster_ca_certificate" {
  description = "Base64-encoded CA cert of the observed cluster."
  value       = google_container_cluster.observed.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

output "observed_kubeconfig_command" {
  value = "gcloud container clusters get-credentials ${google_container_cluster.observed.name} --zone ${var.zone} --project ${var.project_id}"
}
