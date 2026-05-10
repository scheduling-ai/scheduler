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

# --- System node pool: untainted, hosts GKE system pods (kube-dns, etc.) ---
#
# Every chip pool is tainted `scheduler.example.com/managed=true:NoSchedule`
# so the kube-scheduler keeps other workloads off the fake-GPU nodes.
# GKE-managed addons (kube-dns, kube-dns-autoscaler) can't tolerate that
# taint, so without a clean pool they stay Pending and cluster DNS is
# broken. The scheduler plane (k8s-bridge, scheduler-ui, load-generator,
# pomerium) also runs here — keeping it off the chip pools means kubelet
# on the e2-micro chip nodes isn't squeezed when scheduler-plane pods
# co-locate. e2-medium (4 GiB) fits the addons + scheduler plane;
# smaller types OOM kube-dns or leave no room for our services.
# Spot keeps it cheap.
resource "google_container_node_pool" "system" {
  name       = "system"
  cluster    = google_container_cluster.this.name
  location   = var.zone
  node_count = 1

  node_config {
    machine_type = "e2-medium"
    spot         = true
    disk_size_gb = 15
    disk_type    = "pd-standard"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

# --- Artifact Registry: hosts the scheduler container image ---

resource "google_project_service" "artifactregistry" {
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "images" {
  location      = "europe-west4"
  repository_id = "scheduler"
  format        = "DOCKER"
  description   = "Scheduler container images (bridge, UI, generator)."

  depends_on = [google_project_service.artifactregistry]
}

# --- DB node pool: dedicated host for the in-cluster Postgres ---
#
# A single e2-small spot node gives Postgres a guaranteed ~940m CPU and
# ~1.5 GB allocatable memory, isolated from the GKE system pods that
# crowd the system pool.  Single replica + spot means ~3–5 min of
# downtime if the node is evicted; acceptable for the demo.
#
# Tainted so nothing else lands here.  Postgres tolerates it.  Adds
# ~$5/month.
resource "google_container_node_pool" "db" {
  name       = "db"
  cluster    = google_container_cluster.this.name
  location   = var.zone
  node_count = 1

  node_config {
    machine_type = "e2-small"
    spot         = true
    disk_size_gb = 15
    disk_type    = "pd-standard"

    labels = {
      "dedicated" = "db"
    }

    taint {
      key    = "dedicated"
      value  = "db"
      effect = "NO_SCHEDULE"
    }

    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
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

# --- Observed cluster (Kueue-driven; we just watch it) ---------------------
#
# Sibling of `google_container_cluster.this`.  Same chip-pool shape so the
# UI sees a familiar topology under the observe-mode bridge.  No DB pool —
# Kueue persists its state as CRDs in etcd, nothing else here needs Postgres.
#
# This is a SECOND zonal cluster, so it does NOT get GKE's free-tier control
# plane (cluster #1 already consumes it) — expect ~$73/mo just for the
# control plane.  See infra/README.md for the full cost breakdown.

resource "google_container_cluster" "observed" {
  name     = var.observed_cluster_name
  location = var.zone

  logging_config {
    enable_components = []
  }
  monitoring_config {
    enable_components = []
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false
    # Distinct CIDR from the scheduling cluster's master block (172.16.0.0/28).
    master_ipv4_cidr_block = "172.16.0.16/28"
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

resource "google_container_node_pool" "observed_system" {
  name       = "system"
  cluster    = google_container_cluster.observed.name
  location   = var.zone
  node_count = 1

  node_config {
    # e2-standard-2 (2 dedicated vCPU) instead of cluster #1's e2-medium
    # (shared 2 vCPU, bursts to ~940m allocatable).  Cluster #2 also runs
    # Kueue's controller-manager on this node — its 500m CPU request +
    # the ~900m of GKE system pods (kube-dns, konnectivity, metrics, etc.)
    # don't fit on a bursting e2-medium.  Adds ~$6/mo over e2-medium.
    machine_type = "e2-standard-2"
    spot         = true
    disk_size_gb = 15
    disk_type    = "pd-standard"
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

resource "google_container_node_pool" "observed_chip" {
  for_each = local.chip_pools

  name     = each.key
  cluster  = google_container_cluster.observed.name
  location = var.zone

  node_count = var.observed_nodes_per_pool

  node_config {
    machine_type = "e2-micro"
    spot         = true
    disk_size_gb = 15
    disk_type    = "pd-standard"

    labels = {
      "accelerator"                 = each.value.chip_type
      "scheduler.example.com/chips" = tostring(each.value.chips_per_node)
    }

    # Same taint as cluster #1.  Kueue's ResourceFlavor toleration injection
    # adds the matching toleration onto admitted Jobs' pod templates, so
    # workloads still land here.  Without the taint, GKE system addons (and
    # any unrelated default-namespace pods) could squat on chip nodes.
    taint {
      key    = "scheduler.example.com/managed"
      value  = "true"
      effect = "NO_SCHEDULE"
    }

    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
