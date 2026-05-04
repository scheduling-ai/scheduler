//! Pure parsers from k8s objects → scheduler fields.
//!
//! Job and Pod metadata extractors share the same shape, returning
//! `(chips, chip_type, priority, quota, parallelism)`.  Workload
//! extraction dispatches to the right one based on the managed object's
//! kind.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::Job as K8sJob;
use k8s_openapi::api::core::v1::Pod;
use kube::ResourceExt;

use crate::job_store::ManagedObject;

use super::BinderConfig;

/// Extract scheduling metadata from a workload's managed object.
pub(super) fn extract_workload_metadata(
    managed: &ManagedObject,
    config: &BinderConfig,
) -> (u32, String, i32, String, u32) {
    match managed {
        ManagedObject::Job(job) => extract_job_metadata(job, config),
        ManagedObject::Pod(pod) => extract_pod_metadata(pod, config),
    }
}

/// Whether `quota` is one of the names the bridge was started with.
/// Empty quota list = passthrough (no validation), matching the API
/// path's behaviour.
pub(super) fn is_known_quota(config: &BinderConfig, quota: &str) -> bool {
    config.quotas.is_empty() || config.quotas.iter().any(|q| q.name == quota)
}

/// Extract scheduling metadata from a k8s Job manifest.
pub(super) fn extract_job_metadata(
    job: &K8sJob,
    config: &BinderConfig,
) -> (u32, String, i32, String, u32) {
    let spec = job.spec.as_ref();
    let pod_spec = spec.and_then(|s| s.template.spec.as_ref());

    let chips_from_resource = pod_spec
        .and_then(|ps| ps.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get(&config.chip_resource))
        .and_then(|q| q.0.parse::<u32>().ok())
        .unwrap_or(0);
    let chips = if chips_from_resource > 0 {
        chips_from_resource
    } else {
        chips_from_annotation(job.annotations(), config)
    };

    let chip_type = job
        .labels()
        .get(&config.chip_label)
        .cloned()
        .unwrap_or_default();

    let priority = job
        .annotations()
        .get(&config.priority_annotation)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let quota = job
        .annotations()
        .get(&config.quota_annotation)
        .cloned()
        .unwrap_or_else(|| "default".into());

    let parallelism = spec.and_then(|s| s.parallelism).unwrap_or(1) as u32;

    (chips, chip_type, priority, quota, parallelism)
}

/// Extract scheduling metadata from a k8s Pod manifest.
pub(super) fn extract_pod_metadata(
    pod: &Pod,
    config: &BinderConfig,
) -> (u32, String, i32, String, u32) {
    let pod_spec = pod.spec.as_ref();

    let chips_from_resource = pod_spec
        .and_then(|ps| ps.containers.first())
        .and_then(|c| c.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|r| r.get(&config.chip_resource))
        .and_then(|q| q.0.parse::<u32>().ok())
        .unwrap_or(0);
    let chips = if chips_from_resource > 0 {
        chips_from_resource
    } else {
        chips_from_annotation(pod.annotations(), config)
    };

    let chip_type = pod
        .labels()
        .get(&config.chip_label)
        .cloned()
        .unwrap_or_default();

    let priority = pod
        .annotations()
        .get(&config.priority_annotation)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);

    let quota = pod
        .annotations()
        .get(&config.quota_annotation)
        .cloned()
        .unwrap_or_else(|| "default".into());

    // Pods are always a single replica.
    (chips, chip_type, priority, quota, 1)
}

/// Read per-replica chip count from the configured annotation (if any).
/// Falls back to 0 if unset or unparseable — used only as a fallback when
/// the workload's resource request is missing/zero.
pub(super) fn chips_from_annotation(
    annotations: &BTreeMap<String, String>,
    config: &BinderConfig,
) -> u32 {
    config
        .chips_annotation
        .as_deref()
        .and_then(|k| annotations.get(k))
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0)
}
