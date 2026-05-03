//! HTTP API for workload submission and scheduler status.
//!
//! Workloads are submitted as native Kubernetes manifests: either a batch/v1
//! Job or a v1 Pod. No custom schema; scheduling metadata (priority, quota,
//! gang-set) is read from labels/annotations on the object.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Pod;
use tracing::info;

use std::collections::HashSet;
use std::sync::Arc;

use crate::job_store::{JobStatus, ManagedObject, SchedulerState, Workload, WorkloadStore};
use crate::snapshot::{Frame, SnapshotState};

#[derive(Clone)]
struct AppState {
    store: WorkloadStore,
    scheduler: SchedulerState,
    snapshot: SnapshotState,
    /// Names of quotas the bridge knows about, derived from `--quotas`.
    /// Used to fail submissions referencing an unknown quota at the API
    /// rather than letting them poison every solver cycle until backoff
    /// kicks in.
    known_quotas: Arc<HashSet<String>>,
    quota_annotation: String,
}

/// Build the axum router.
pub fn router(
    store: WorkloadStore,
    scheduler: SchedulerState,
    snapshot: SnapshotState,
    known_quotas: HashSet<String>,
    quota_annotation: String,
) -> Router {
    let state = AppState {
        store,
        scheduler,
        snapshot,
        known_quotas: Arc::new(known_quotas),
        quota_annotation,
    };
    Router::new()
        .route("/jobs", post(submit_workload).get(list_workloads))
        .route("/jobs/{name}", get(get_workload).delete(delete_workload))
        .route("/status", get(get_status))
        .route("/status/{name}", get(get_job_status))
        .route("/snapshot", get(get_snapshot))
        .route("/debug/sentry", post(debug_sentry))
        .with_state(state)
}

/// Verifies Sentry SDK init + DSN reachability from inside the deployed
/// pod. Captures one info message and one explicit error event, flushes,
/// and returns the event IDs so the caller can confirm delivery via the
/// Sentry MCP / web UI.
///
/// Gated by `SENTRY_DEBUG=1` so we don't ship a permanent test path that
/// pollutes the inbox if hit by accident. Returns 404 otherwise.
async fn debug_sentry() -> Result<Json<serde_json::Value>, StatusCode> {
    if std::env::var("SENTRY_DEBUG").as_deref() != Ok("1") {
        return Err(StatusCode::NOT_FOUND);
    }

    let marker = format!(
        "sentry-test-{}-{}",
        std::env::var("GIT_SHA").unwrap_or_else(|_| "dev".into()),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    );

    let info_id = sentry::capture_message(
        &format!("k8s-bridge sentry test ({marker})"),
        sentry::Level::Info,
    );

    #[derive(Debug)]
    struct DebugError(String);
    impl std::fmt::Display for DebugError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for DebugError {}

    let err = DebugError(format!("intentional sentry test exception ({marker})"));
    let exc_id = sentry::capture_error(&err);

    if let Some(client) = sentry::Hub::current().client() {
        client.flush(Some(std::time::Duration::from_secs(5)));
    }

    Ok(Json(serde_json::json!({
        "marker": marker,
        "message_event_id": info_id.to_string(),
        "exception_event_id": exc_id.to_string(),
        "dsn_present": std::env::var("SENTRY_DSN")
            .map(|s| !s.is_empty())
            .unwrap_or(false),
    })))
}

/// Accept a raw JSON body and dispatch based on `kind`.
async fn submit_workload(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    // Peek at `kind` to determine the object type.
    let value: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid JSON: {e}")))?;

    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");

    match kind {
        "Job" => {
            let job: Job = serde_json::from_value(value)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid Job: {e}")))?;
            submit_job(state, job).await
        }
        "Pod" => {
            let pod: Pod = serde_json::from_value(value)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid Pod: {e}")))?;
            submit_pod(state, pod).await
        }
        "" => Err((
            StatusCode::BAD_REQUEST,
            "manifest must include 'kind' field (Job or Pod)".into(),
        )),
        other => Err((
            StatusCode::BAD_REQUEST,
            format!("unsupported kind '{other}', expected Job or Pod"),
        )),
    }
}

async fn submit_job(
    state: AppState,
    job: Job,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    let name = job.metadata.name.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Job must have metadata.name".into(),
        )
    })?;

    let is_suspended = job.spec.as_ref().and_then(|s| s.suspend).unwrap_or(false);
    if !is_suspended {
        return Err((
            StatusCode::BAD_REQUEST,
            "Job must be submitted with spec.suspend: true".into(),
        ));
    }

    // Reject submissions referencing an unknown quota at the door — a
    // single bad quota name otherwise stalls every solver cycle until
    // backoff kicks in.
    if let Some(quota) = job
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(&state.quota_annotation))
        && !state.known_quotas.is_empty()
        && !state.known_quotas.contains(quota)
    {
        let mut known: Vec<&str> = state.known_quotas.iter().map(String::as_str).collect();
        known.sort();
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "Job references unknown quota '{quota}'.  Known quotas: {}",
                known.join(", ")
            ),
        ));
    }

    let workload = Workload {
        managed: ManagedObject::Job(Box::new(job)),
        generation: 0,
        consecutive_failures: 0,
    };
    match state.store.insert_new(name.clone(), workload).await {
        Ok(true) => {
            info!(workload = %name, kind = "Job", "workload submitted");
            Ok((
                StatusCode::CREATED,
                Json(serde_json::json!({"name": name, "kind": "Job", "status": "queued"})),
            ))
        }
        Ok(false) => Err((
            StatusCode::CONFLICT,
            format!("workload '{name}' already exists"),
        )),
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("persistence error: {e}"),
        )),
    }
}

/// Bare Pod submissions are rejected: the scheduler can only re-create or
/// suspend workloads that have an owner controller (Job, Deployment).  An
/// orphan Pod that gets preempted has nowhere to come back from.  Submit
/// via Job, or let a Deployment / ReplicaSet / StatefulSet own the Pod
/// and the scheduler will pick it up via the reflector path.
async fn submit_pod(
    _state: AppState,
    _pod: Pod,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    Err((
        StatusCode::BAD_REQUEST,
        "Bare Pod submissions are not supported.  Submit a Job (with \
         spec.suspend: true), or run the Pod under a Deployment / \
         ReplicaSet / StatefulSet — the scheduler will discover it via \
         the cluster reflector."
            .into(),
    ))
}

async fn list_workloads(State(state): State<AppState>) -> Json<Vec<String>> {
    Json(state.store.keys().await)
}

async fn get_workload(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let workload = state.store.get(&name).await.ok_or(StatusCode::NOT_FOUND)?;
    let value = match &workload.managed {
        ManagedObject::Job(job) => serde_json::to_value(job.as_ref()).unwrap_or_default(),
        ManagedObject::Pod(pod) => serde_json::to_value(pod.as_ref()).unwrap_or_default(),
    };
    Ok(Json(value))
}

async fn delete_workload(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    match state.store.remove(&name).await {
        Ok(true) => {
            info!(workload = %name, "workload deleted");
            Ok(StatusCode::NO_CONTENT)
        }
        Ok(false) => Ok(StatusCode::NOT_FOUND),
        Err(e) => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            format!("persistence error: {e}"),
        )),
    }
}

/// Return the status of all known jobs: queue position, eviction risk, etc.
async fn get_status(State(state): State<AppState>) -> Json<Vec<JobStatus>> {
    let sched = state.scheduler.lock().await;
    Json(sched.job_statuses())
}

/// Return the status of a single job by name.
async fn get_job_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<JobStatus>, StatusCode> {
    let sched = state.scheduler.lock().await;
    sched
        .job_statuses()
        .into_iter()
        .find(|j| j.name == name)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Return the latest per-tick Frame snapshot, consumed by the UI's live mode.
/// 404 until the binder has completed its first solve.
async fn get_snapshot(State(state): State<AppState>) -> Result<Json<Frame>, StatusCode> {
    state
        .snapshot
        .lock()
        .await
        .clone()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}
