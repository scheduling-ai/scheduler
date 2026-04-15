terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "europe-west4"
}

# --- APIs ---

resource "google_project_service" "container" {
  service            = "container.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# --- Cloud NAT (private nodes need outbound internet for image pulls) ---

resource "google_compute_router" "this" {
  name    = "scheduler-router"
  network = "default"
  region  = "europe-west4"

  depends_on = [google_project_service.compute]
}

resource "google_compute_router_nat" "this" {
  name   = "scheduler-nat"
  router = google_compute_router.this.name
  region = "europe-west4"

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

# --- GKE cluster (zonal, free tier, private nodes) ---

resource "google_container_cluster" "this" {
  name     = var.cluster_name
  location = var.zone

  # No logging/monitoring agents — e2-micro has no room for them.
  logging_config {
    enable_components = []
  }
  monitoring_config {
    enable_components = []
  }

  # Private nodes: no external IPs, avoids IN_USE_ADDRESSES quota.
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  deletion_protection = false

  initial_node_count       = 1
  remove_default_node_pool = true

  depends_on = [
    google_project_service.container,
    google_project_service.compute,
    google_compute_router_nat.this,
  ]
}

# One homogeneous pool per chip type. Labels set at node registration —
# no patcher DaemonSet, no race with kubelet, spot replacements inherit labels.
locals {
  chip_pools = {
    h200 = { chip_type = "H200", chips_per_node = 8 }
    h100 = { chip_type = "H100", chips_per_node = 8 }
    a100 = { chip_type = "A100", chips_per_node = 16 }
    l40s = { chip_type = "L40S", chips_per_node = 4 }
  }
}

resource "google_container_node_pool" "chip" {
  for_each = local.chip_pools

  name     = each.key
  cluster  = google_container_cluster.this.name
  location = var.zone

  node_count = var.nodes_per_pool

  node_config {
    machine_type = "e2-micro"
    spot         = true
    disk_size_gb = 15
    disk_type    = "pd-standard"

    labels = {
      "accelerator"                 = each.value.chip_type
      "scheduler.example.com/chips" = tostring(each.value.chips_per_node)
    }

    taint {
      key    = "scheduler.example.com/managed"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
