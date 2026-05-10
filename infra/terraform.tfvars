project_id = "scheduler-testing-project"

# Cluster #1 chip-pool size, halved from the default 8.  europe-west4's
# INSTANCES quota is currently 34, fully consumed by the original
# 8-per-pool layout; sharing the region with the observed cluster
# requires shrinking both.  Restore to 8 once the quota bump lands —
# nothing else in the repo cares about this number.
nodes_per_pool = 4
