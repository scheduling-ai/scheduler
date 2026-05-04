//! Compare solver input and output to derive the actions the apply
//! step has to perform: new placements, suspensions, unsuspensions, and
//! the still-queued ordering.

use std::collections::HashMap;

use crate::solver_types::{Phase, ScheduleResult, SolverRequest};

/// Derived actions from diffing the solver's input and output.
pub(super) struct ScheduleDiff {
    /// pod_name -> (cluster_name, {node_name -> replica_count}) for newly placed pods.
    pub(super) assign: HashMap<String, (String, HashMap<String, u32>)>,
    /// pod_names to suspend (running -> suspended).
    pub(super) suspend: Vec<String>,
    /// pod_name -> {node_name -> replica_count} for unsuspended pods.
    pub(super) unsuspend: HashMap<String, HashMap<String, u32>>,
    /// pod_names still waiting, in queue-priority order.
    pub(super) queue_order: Vec<String>,
}

/// Compare solver input and output to determine what actions to take.
pub(super) fn diff_schedule(request: &SolverRequest, result: &ScheduleResult) -> ScheduleDiff {
    let mut assign: HashMap<String, (String, HashMap<String, u32>)> = HashMap::new();
    let mut suspend: Vec<String> = Vec::new();
    let mut unsuspend: HashMap<String, HashMap<String, u32>> = HashMap::new();
    let mut queue_order: Vec<String> = Vec::new();

    for (name, out_pod) in &result.pods {
        let in_pod = match request.pods.get(name) {
            Some(p) => p,
            None => continue,
        };

        // Detect suspension: was running ON A CLUSTER, now suspended.
        // Only pods with a cluster assignment can be suspended — queued pods
        // (cluster=None) have nothing on any cluster to suspend.
        let was_on_cluster = in_pod.cluster.is_some();
        let was_running = in_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Running);
        let now_suspended = out_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Suspended);
        if was_on_cluster && was_running && now_suspended {
            suspend.push(name.clone());
            continue;
        }

        // Count newly assigned nodes (replicas that gained a node).
        // This must happen BEFORE the still_pending check so that partial
        // placements (some replicas placed, some not) are not silently lost.
        let mut new_nodes: HashMap<String, u32> = HashMap::new();
        for (in_r, out_r) in in_pod
            .statuses_by_replica
            .iter()
            .zip(out_pod.statuses_by_replica.iter())
        {
            if in_r.node.is_none()
                && let Some(ref node) = out_r.node
            {
                *new_nodes.entry(node.clone()).or_insert(0) += 1;
            }
        }

        if !new_nodes.is_empty() {
            let was_suspended = in_pod
                .statuses_by_replica
                .iter()
                .any(|r| r.phase == Phase::Suspended);
            if was_suspended {
                unsuspend.insert(name.clone(), new_nodes);
            } else {
                let cluster = out_pod.cluster.clone().unwrap_or_default();
                assign.insert(name.clone(), (cluster, new_nodes));
            }
            continue;
        }

        // Still-queued: all replicas pending (no new nodes assigned).
        let still_pending = out_pod
            .statuses_by_replica
            .iter()
            .any(|r| r.phase == Phase::Running && r.node.is_none());
        if still_pending {
            queue_order.push(name.clone());
        }
    }

    ScheduleDiff {
        assign,
        suspend,
        unsuspend,
        queue_order,
    }
}
