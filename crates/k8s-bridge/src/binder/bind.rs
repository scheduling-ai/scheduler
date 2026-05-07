//! Pod → Node binding via the k8s Binding API.
//!
//! Run after the apply phase: `apply` may have just unsuspended a Job or
//! created one, leaving its Pods in `Pending` with our `schedulerName`.
//! `bind_pending_pods` walks all clusters, groups Pending Pods by their
//! logical workload, and looks up the target nodes in `placement_shadow`
//! to bind each Pod via `POST /pods/{name}/binding`.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::{Binding, ObjectReference, Pod};
use kube::{Api, Client, ResourceExt, api::PostParams};
use tracing::{info, warn};

use super::apply::timed_rpc;
use super::{BinderConfig, ClusterRuntime, PlacementShadow};

/// Bind a Pending pod to a specific node via the k8s Binding API.
///
/// This is the mechanism by which our binder acts as a scheduler: instead of
/// relying on the k8s scheduler to find a node, we call
/// `POST /api/v1/namespaces/{ns}/pods/{name}/binding` directly.
pub(super) async fn bind_pod(
    client: &Client,
    ns: &str,
    pod_name: &str,
    node_name: &str,
) -> (Result<()>, u64) {
    let binding = Binding {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(pod_name.to_owned()),
            ..Default::default()
        },
        target: ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("Node".into()),
            name: Some(node_name.to_owned()),
            ..Default::default()
        },
    };
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let (result, rpc_ms) = timed_rpc(pods_api.create_subresource::<Binding, serde_json::Value>(
        "binding",
        pod_name,
        &PostParams::default(),
        &binding,
    ))
    .await;
    (
        result.map(|_| ()).context("failed to bind pod to node"),
        rpc_ms,
    )
}

/// Bind any Pending pods that have our `schedulerName` and no `nodeName` yet.
///
/// Called each cycle before the solver request is built.  The
/// `placement_shadow` map tells us which node each job's replicas
/// should land on; we distribute Pending pods across those nodes in
/// name-sorted order for stability.
pub(super) async fn bind_pending_pods(
    runtimes: &[ClusterRuntime],
    clients: &HashMap<String, Client>,
    config: &BinderConfig,
    placement_shadow: &PlacementShadow,
) {
    for rt in runtimes {
        let Some(client) = clients.get(&rt.name) else {
            continue;
        };
        let pod_reader = rt.pod_reader();
        let job_reader = rt.job_reader();

        // Collect Pending, unbound pods that we manage.  Owner kind doesn't
        // matter here — Job-spawned pods, ReplicaSet-spawned pods (KEDA /
        // Deployment path), and bare managed Pods all need the same Binding
        // API call to land them on the solver's chosen node.
        let mut unbound: Vec<Arc<Pod>> = pod_reader
            .state()
            .into_iter()
            .filter(|pod| {
                let our_scheduler = pod.spec.as_ref().and_then(|s| s.scheduler_name.as_deref())
                    == Some(config.scheduler_name.as_str());
                let pending =
                    pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Pending");
                let no_node = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_ref())
                    .is_none();
                let is_managed = pod
                    .labels()
                    .get(&config.managed_by_label)
                    .map(|v| v == &config.managed_by_value)
                    .unwrap_or(false);
                our_scheduler && pending && no_node && is_managed
            })
            .collect();

        if unbound.is_empty() {
            continue;
        }

        // Group by the same key the solver uses for SolverPod:
        //
        // - Job-owned pods: all replicas of a Job share one SolverPod
        //   (`job-name` label value).  Read it from the pod's label, or
        //   fall back to the parent Job's label if the template omits
        //   it (the bridge only auto-injects `managed-by`).
        //
        // - Non-Job-owned managed pods (Deployment / ReplicaSet /
        //   KEDA-driven): each k8s Pod is its own single-replica
        //   SolverPod, keyed by k8s pod name.  The `job-name` label is
        //   *not* unique across replicas — a Deployment stamps the same
        //   label on every replica's pod template — so using it would
        //   collapse N pending pods into one group and silently drop
        //   N-1 placements at the zip below.
        let mut by_workload: HashMap<String, Vec<Arc<Pod>>> = HashMap::new();
        for pod in unbound.drain(..) {
            let job_uid = pod
                .metadata
                .owner_references
                .as_ref()
                .and_then(|refs| refs.iter().find(|r| r.kind == "Job"))
                .map(|r| r.uid.as_str())
                .unwrap_or("");
            let wl_name = if job_uid.is_empty() {
                // Non-Job-owned: SolverPod key == k8s pod name.
                pod.name_any()
            } else {
                // Job-owned: SolverPod key == parent Job's `job-name` label.
                let from_pod_label = pod.labels().get(&config.job_name_label).cloned();
                match from_pod_label {
                    Some(name) => name,
                    None => {
                        let parent = job_reader
                            .state()
                            .iter()
                            .find(|j| j.metadata.uid.as_deref() == Some(job_uid))
                            .cloned();
                        match parent {
                            Some(job) => job
                                .labels()
                                .get(&config.job_name_label)
                                .cloned()
                                .unwrap_or_else(|| job.name_any()),
                            None => pod.name_any(),
                        }
                    }
                }
            };
            by_workload.entry(wl_name).or_default().push(pod);
        }

        for (wl_name, mut pods) in by_workload {
            let Some((node_counts, _, _)) = placement_shadow.get(&wl_name) else {
                continue;
            };

            // Stable assignment: sort pods by name, expand node_counts into a
            // flat list, zip together.
            pods.sort_by_key(|p| p.name_any());
            let nodes_flat: Vec<&str> = node_counts
                .iter()
                .flat_map(|(node, &count)| std::iter::repeat_n(node.as_str(), count as usize))
                .collect();

            for (pod, node_name) in pods.iter().zip(nodes_flat.iter()) {
                let pod_name = pod.name_any();
                let ns = pod.namespace().unwrap_or_else(|| "default".into());
                let client = client.clone();
                let node_name = node_name.to_string();
                let wl = wl_name.clone();

                tokio::spawn(async move {
                    let (result, rpc_ms) = bind_pod(&client, &ns, &pod_name, &node_name).await;
                    match result {
                        Ok(()) => info!(
                            workload = wl,
                            pod = pod_name,
                            node = node_name,
                            rpc_ms,
                            "bound pod to node"
                        ),
                        Err(e) => warn!(
                            workload = wl,
                            pod = pod_name,
                            node = node_name,
                            rpc_ms,
                            "bind failed: {e}"
                        ),
                    }
                });
            }
        }
    }
}
