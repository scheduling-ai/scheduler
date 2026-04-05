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

use crate::job_store::{
    JobStatus, ManagedObject, SchedulerState, Workload, WorkloadState, WorkloadStore,
};

#[derive(Clone)]
struct AppState {
    store: WorkloadStore,
    scheduler: SchedulerState,
}

/// Build the axum router with the workload store and scheduler state.
pub fn router(store: WorkloadStore, scheduler: SchedulerState) -> Router {
    let state = AppState { store, scheduler };
    Router::new()
        .route("/jobs", post(submit_workload).get(list_workloads))
        .route("/jobs/{name}", get(get_workload).delete(delete_workload))
        .route("/status", get(get_status))
        .route("/status/{name}", get(get_job_status))
        .with_state(state)
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

    let mut s = state.store.lock().await;
    if s.contains_key(&name) {
        return Err((
            StatusCode::CONFLICT,
            format!("workload '{name}' already exists"),
        ));
    }
    info!(workload = %name, kind = "Job", "workload submitted");
    s.insert(
        name.clone(),
        Workload {
            managed: ManagedObject::Job(Box::new(job)),
            state: WorkloadState::Queued,
            generation: 0,
            consecutive_failures: 0,
        },
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"name": name, "kind": "Job", "status": "queued"})),
    ))
}

async fn submit_pod(
    state: AppState,
    pod: Pod,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    let name = pod.metadata.name.clone().ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Pod must have metadata.name".into(),
        )
    })?;

    let mut s = state.store.lock().await;
    if s.contains_key(&name) {
        return Err((
            StatusCode::CONFLICT,
            format!("workload '{name}' already exists"),
        ));
    }
    info!(workload = %name, kind = "Pod", "workload submitted");
    s.insert(
        name.clone(),
        Workload {
            managed: ManagedObject::Pod(Box::new(pod)),
            state: WorkloadState::Queued,
            generation: 0,
            consecutive_failures: 0,
        },
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({"name": name, "kind": "Pod", "status": "queued"})),
    ))
}

async fn list_workloads(State(state): State<AppState>) -> Json<Vec<String>> {
    let s = state.store.lock().await;
    Json(s.keys().cloned().collect())
}

async fn get_workload(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let s = state.store.lock().await;
    let workload = s.get(&name).ok_or(StatusCode::NOT_FOUND)?;
    let value = match &workload.managed {
        ManagedObject::Job(job) => serde_json::to_value(job.as_ref()).unwrap_or_default(),
        ManagedObject::Pod(pod) => serde_json::to_value(pod.as_ref()).unwrap_or_default(),
    };
    Ok(Json(value))
}

async fn delete_workload(State(state): State<AppState>, Path(name): Path<String>) -> StatusCode {
    let mut s = state.store.lock().await;
    if s.remove(&name).is_some() {
        info!(workload = %name, "workload deleted");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
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
