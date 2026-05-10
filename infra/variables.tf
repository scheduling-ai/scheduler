variable "project_id" {
  type = string
}

variable "zone" {
  type    = string
  default = "europe-west4-a"
}

variable "cluster_name" {
  type    = string
  default = "scheduler"
}

variable "nodes_per_pool" {
  type    = number
  default = 8
}

# Second cluster — runs Kueue natively and is observed (read-only) by the
# scheduler-plane bridge. Sibling of `cluster_name`; same shape minus the
# DB pool (Kueue stores its state in CRDs, no Postgres needed).
variable "observed_cluster_name" {
  type    = string
  default = "scheduler-observed"
}

# Per-pool node count for the observed cluster.  Kept independent from
# `nodes_per_pool` so the two clusters can scale separately while the
# europe-west4 INSTANCES quota is tight (currently 34).  Once a quota
# bump lands, raise this to match `nodes_per_pool`.
variable "observed_nodes_per_pool" {
  type    = number
  default = 3
}
