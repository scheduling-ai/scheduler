//! Apply phase: turn a [`ScheduleDiff`] into k8s API calls.
//!
//! Three flavours of action — suspensions (patch Job.spec.suspend=true or
//! delete Pod), unsuspensions (patch Job.spec.suspend=false), and new
//! placements (create the workload on the target cluster).  Pod binding to
//! specific nodes happens later in `bind_pending_pods`; this module only
//! gets the workload onto the cluster.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use k8s_openapi::api::batch::v1::Job as K8sJob;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api, Client, ResourceExt,
    api::{DeleteParams, Patch, PatchParams, PostParams},
    runtime::reflector,
};
use tracing::{info, warn};

use crate::job_store::{ManagedObject, RemoveOutcome, Workload, WorkloadStore};

use super::BinderConfig;
use super::diff::ScheduleDiff;

/// Per-RPC timeout for kube API calls in the apply / bind paths.
///
/// kube-rs has no client-side timeout by default, so without this the
/// binder loop can stall indefinitely on a single hung RPC (e.g. a
/// freshly-spun-up kind cluster API server taking minutes to respond on
/// the very first non-watch request — see CircleCI pipeline #42).  On
/// timeout we surface an error; the caller logs+continues and the
/// workload is retried next cycle (the store entry is only removed
/// after a successful create).
pub(super) const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Run a kube RPC with [`RPC_TIMEOUT`] and elapsed-time tracking.
///
/// Returns `(outcome, rpc_ms)` so callers can surface timing on both
/// success and failure paths — granular per-RPC timing was missing from
/// the previous diagnostics, which only logged the aggregate `apply_ms`
/// for the whole cycle.
pub(super) async fn timed_rpc<F, T>(fut: F) -> (Result<T>, u64)
where
    F: std::future::Future<Output = std::result::Result<T, kube::Error>>,
{
    let started = std::time::Instant::now();
    let outcome = match tokio::time::timeout(RPC_TIMEOUT, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(anyhow::Error::from(e)),
        Err(_) => Err(anyhow::anyhow!(
            "kube RPC timed out after {}s",
            RPC_TIMEOUT.as_secs()
        )),
    };
    (outcome, started.elapsed().as_millis() as u64)
}

/// Everything needed by [`apply_assignments_multi`].
pub(super) struct ApplyContext {
    pub(super) clients: HashMap<String, Client>,
    pub(super) dry_run: bool,
    pub(super) config: BinderConfig,
    pub(super) store_snapshot: HashMap<String, Workload>,
    pub(super) store: Option<WorkloadStore>,
    pub(super) cluster_job_readers: HashMap<String, reflector::Store<K8sJob>>,
    pub(super) cluster_pod_readers: HashMap<String, reflector::Store<Pod>>,
}

/// Apply all solver actions: suspensions, unsuspensions, and new assignments.
pub(super) async fn apply_assignments_multi(diff: &ScheduleDiff, ctx: &ApplyContext) {
    let mut join_set = tokio::task::JoinSet::new();

    for pod_name in &diff.suspend {
        apply_suspension(pod_name, ctx, &mut join_set).await;
    }

    for (pod_name, node_counts) in &diff.unsuspend {
        apply_unsuspension(pod_name, node_counts, ctx, &mut join_set).await;
    }

    for (pod_name, (cluster_name, node_counts)) in &diff.assign {
        let Some(client) = ctx.clients.get(cluster_name) else {
            warn!(
                workload = pod_name,
                cluster = cluster_name,
                "no client for assigned cluster, skipping"
            );
            continue;
        };

        if let Some(workload) = ctx.store_snapshot.get(pod_name) {
            if ctx.dry_run {
                info!(
                    workload = pod_name,
                    cluster = cluster_name,
                    "would create workload on cluster"
                );
            } else {
                let (result, rpc_ms) = match &workload.managed {
                    ManagedObject::Job(job) => create_k8s_job(job, client, &ctx.config).await,
                    ManagedObject::Pod(pod) => {
                        let node_name = node_counts.keys().next().map(String::as_str).unwrap_or("");
                        create_k8s_pod(pod, client, node_name, &ctx.config).await
                    }
                };
                match result {
                    Ok(()) => {
                        info!(
                            workload = pod_name,
                            cluster = cluster_name,
                            rpc_ms,
                            "created workload on cluster"
                        );
                    }
                    Err(e) => {
                        warn!(
                            workload = pod_name,
                            cluster = cluster_name,
                            rpc_ms,
                            "failed to create workload: {e}"
                        );
                        continue;
                    }
                }
            }
            if let Some(s) = &ctx.store {
                remove_if_generation_matches(s, pod_name, &ctx.store_snapshot).await;
            }
        }
    }

    while join_set.join_next().await.is_some() {}
}

/// Remove a workload from the store only if its generation matches the
/// snapshot the solver acted on. If the workload was modified (e.g.,
/// deleted and resubmitted) between snapshot and apply, the removal is
/// skipped and the workload will be retried next cycle.
async fn remove_if_generation_matches(
    store: &WorkloadStore,
    name: &str,
    snapshot: &HashMap<String, Workload>,
) {
    let expected_gen = match snapshot.get(name) {
        Some(wl) => wl.generation,
        None => return,
    };
    match store.remove_if_generation_matches(name, expected_gen).await {
        Ok(RemoveOutcome::Removed) | Ok(RemoveOutcome::NotPresent) => {}
        Ok(RemoveOutcome::GenerationMismatch { actual }) => {
            warn!(
                workload = name,
                expected = expected_gen,
                actual,
                "generation mismatch, skipping store removal"
            );
        }
        Err(e) => warn!(workload = name, "persistence remove failed: {e}"),
    }
}

/// Suspend a workload.
///
/// - Jobs: patch `spec.suspend = true` (k8s deletes pods atomically).
/// - Pods: delete the Pod from the cluster, re-enter store as Suspended.
async fn apply_suspension(
    wl_name: &str,
    ctx: &ApplyContext,
    join_set: &mut tokio::task::JoinSet<()>,
) {
    if ctx.dry_run {
        info!(workload = wl_name, "would suspend");
        return;
    }

    for (cluster_name, job_reader) in &ctx.cluster_job_readers {
        let target = job_reader
            .state()
            .iter()
            .find(|j| {
                j.labels()
                    .get(&ctx.config.job_name_label)
                    .map(|n| n == wl_name)
                    .unwrap_or_else(|| j.name_any() == wl_name)
            })
            .cloned();

        let Some(job) = target else { continue };

        let k8s_name = job.name_any();
        let ns = job.namespace().unwrap_or_else(|| "default".into());
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();

        join_set.spawn(async move {
            let jobs_api: Api<K8sJob> = Api::namespaced(client, &ns);
            let patch = serde_json::json!({
                "spec": { "suspend": true }
            });
            let (result, rpc_ms) =
                timed_rpc(jobs_api.patch(&k8s_name, &PatchParams::default(), &Patch::Merge(patch)))
                    .await;
            match result {
                Ok(_) => info!(
                    cluster = cluster_owned,
                    rpc_ms, "suspended job {ns}/{k8s_name}"
                ),
                Err(e) => warn!(
                    cluster = cluster_owned,
                    rpc_ms, "failed to suspend job {ns}/{k8s_name}: {e}"
                ),
            }
        });
        return;
    }

    // Pod path: deletion-only.  Pods we manage that aren't owned by a Job
    // come from a Deployment / ReplicaSet / StatefulSet (e.g. KEDA-driven
    // inference); the owner controller will respawn them on the same
    // cluster with a fresh template.  We don't store the spec — the
    // owner is the source of truth.
    for (cluster_name, pod_reader) in &ctx.cluster_pod_readers {
        let target = pod_reader
            .state()
            .iter()
            .find(|p| {
                let is_managed = p
                    .labels()
                    .get(&ctx.config.managed_by_label)
                    .map(|v| v == &ctx.config.managed_by_value)
                    .unwrap_or(false);
                if !is_managed {
                    return false;
                }
                let owned_by_job = p
                    .metadata
                    .owner_references
                    .as_ref()
                    .map(|refs| refs.iter().any(|r| r.kind == "Job"))
                    .unwrap_or(false);
                if owned_by_job {
                    return false;
                }
                // SolverPod key for non-Job-owned managed pods is the
                // k8s pod name (see solver_request.rs's standalone-Pod
                // loop).  The `job-name` label is shared across replicas
                // of a Deployment and would mis-target.
                p.name_any() == wl_name
            })
            .cloned();

        let Some(pod) = target else { continue };

        let k8s_name = pod.name_any();
        let ns = pod.namespace().unwrap_or_else(|| "default".into());
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();

        join_set.spawn(async move {
            let pods_api: Api<Pod> = Api::namespaced(client, &ns);
            let (result, rpc_ms) =
                timed_rpc(pods_api.delete(&k8s_name, &DeleteParams::default())).await;
            match result {
                Ok(_) => info!(
                    cluster = cluster_owned,
                    rpc_ms, "suspended (deleted) pod {ns}/{k8s_name}"
                ),
                Err(e) => warn!(
                    cluster = cluster_owned,
                    rpc_ms, "failed to delete pod {ns}/{k8s_name} for suspension: {e}"
                ),
            }
        });
        return;
    }
}

/// Unsuspend a Job.
///
/// Flips `spec.suspend = false` on the Job; pods go Pending with our
/// `schedulerName` and the next `bind_pending_pods` pass binds them to
/// the nodes recorded in `placement_shadow`.
///
/// Pods don't have a non-Job unsuspension path: KEDA-style Pods aren't
/// stored on suspension (the owner Deployment respawns them), so when
/// they reappear they enter via the reflector path as fresh queued
/// workloads — there's nothing for us to "unsuspend".
async fn apply_unsuspension(
    wl_name: &str,
    _node_counts: &HashMap<String, u32>,
    ctx: &ApplyContext,
    join_set: &mut tokio::task::JoinSet<()>,
) {
    if ctx.dry_run {
        info!(workload = wl_name, "would unsuspend");
        return;
    }

    for (cluster_name, job_reader) in &ctx.cluster_job_readers {
        let target = job_reader
            .state()
            .iter()
            .find(|j| {
                j.labels()
                    .get(&ctx.config.job_name_label)
                    .map(|n| n == wl_name)
                    .unwrap_or_else(|| j.name_any() == wl_name)
            })
            .cloned();

        let Some(job) = target else { continue };

        let ns = job.namespace().unwrap_or_else(|| "default".into());
        let k8s_name = job.name_any();
        let Some(client) = ctx.clients.get(cluster_name).cloned() else {
            continue;
        };
        let cluster_owned = cluster_name.clone();

        join_set.spawn(async move {
            let jobs_api: Api<K8sJob> = Api::namespaced(client, &ns);
            let patch = serde_json::json!({ "spec": { "suspend": false } });
            let (result, rpc_ms) =
                timed_rpc(jobs_api.patch(&k8s_name, &PatchParams::default(), &Patch::Merge(patch)))
                    .await;
            match result {
                Ok(_) => info!(
                    cluster = cluster_owned,
                    rpc_ms, "unsuspended job {ns}/{k8s_name}"
                ),
                Err(e) => warn!(
                    cluster = cluster_owned,
                    rpc_ms, "failed to unsuspend job {ns}/{k8s_name}: {e}"
                ),
            }
        });
        return;
    }
}

/// Create a batch/v1 Job on the target cluster.
///
/// The pod template gets `spec.schedulerName` set to our scheduler name so
/// that the k8s default scheduler ignores the resulting pods.  The binder
/// will bind each pod to its target node via the Binding API in the next
/// `bind_pending_pods` pass.
async fn create_k8s_job(
    submitted_job: &K8sJob,
    client: &Client,
    config: &BinderConfig,
) -> (Result<()>, u64) {
    let ns = submitted_job
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default");
    let jobs_api: Api<K8sJob> = Api::namespaced(client.clone(), ns);

    let mut job = submitted_job.clone();

    job.metadata.resource_version = None;
    job.metadata.uid = None;
    job.status = None;

    let labels = job.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        config.managed_by_label.clone(),
        config.managed_by_value.clone(),
    );

    if let Some(ref mut spec) = job.spec {
        let pod_spec = spec.template.spec.get_or_insert_with(Default::default);
        pod_spec.scheduler_name = Some(config.scheduler_name.clone());

        let template_labels = spec
            .template
            .metadata
            .get_or_insert_with(Default::default)
            .labels
            .get_or_insert_with(Default::default);
        template_labels.insert(
            config.managed_by_label.clone(),
            config.managed_by_value.clone(),
        );

        spec.suspend = Some(false);
    }

    let (result, rpc_ms) = timed_rpc(jobs_api.create(&PostParams::default(), &job)).await;
    (
        result
            .map(|_| ())
            .context("failed to create Job on cluster"),
        rpc_ms,
    )
}

/// Create a standalone v1 Pod on the target cluster.
///
/// `spec.nodeName` is set directly, bypassing the k8s scheduler entirely.
/// This is the correct approach for standalone pods because we control their
/// creation and can set the target node atomically.
async fn create_k8s_pod(
    submitted_pod: &Pod,
    client: &Client,
    node_name: &str,
    config: &BinderConfig,
) -> (Result<()>, u64) {
    let ns = submitted_pod
        .metadata
        .namespace
        .as_deref()
        .unwrap_or("default");
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), ns);

    let mut pod = submitted_pod.clone();

    pod.metadata.resource_version = None;
    pod.metadata.uid = None;
    pod.metadata.creation_timestamp = None;
    pod.status = None;

    let labels = pod.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(
        config.managed_by_label.clone(),
        config.managed_by_value.clone(),
    );

    let pod_spec = pod.spec.get_or_insert_with(Default::default);
    pod_spec.node_name = Some(node_name.to_owned());

    let (result, rpc_ms) = timed_rpc(pods_api.create(&PostParams::default(), &pod)).await;
    (
        result
            .map(|_| ())
            .context("failed to create Pod on cluster"),
        rpc_ms,
    )
}
