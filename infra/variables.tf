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

variable "node_count" {
  type    = number
  default = 32
}

