mod api;
mod binder;
mod job_store;

mod observer;
mod persistence;
mod snapshot;
mod solver;
mod solver_types;

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "k8s-bridge",
    about = "Kubernetes bridge: watches cluster state and executes scheduler decisions"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Watch cluster workloads, call the solver, and manage lifecycle.
    Bind {
        /// Log placement decisions without actually creating/patching workloads.
        #[arg(long)]
        dry_run: bool,
        /// Clusters to manage, as name:context pairs. The context is the
        /// kubeconfig context name. May be repeated for multi-cluster
        /// operation. Use name (without a colon) to use the current/default
        /// kubeconfig context.
        ///
        /// Examples:
        ///   --cluster local
        ///   --cluster us-east:kind-us-east --cluster eu-west:kind-eu-west
        #[arg(long = "cluster", required = true)]
        clusters: Vec<String>,
        /// Node label key for chip/accelerator type.
        #[arg(long, default_value = "accelerator")]
        chip_label: String,
        /// Resource name for chip/GPU capacity.
        #[arg(long, default_value = "nvidia.com/gpu")]
        chip_resource: String,
        /// If set, read per-node chip count from this node label instead of
        /// the extended resource. Used by test clusters without a device plugin.
        #[arg(long)]
        chip_count_label: Option<String>,
        /// If set, read per-replica chip count from this annotation on the
        /// Job/Pod when the container's `resources.requests[chip_resource]`
        /// is missing or zero. For demo clusters where requesting
        /// `nvidia.com/gpu` would cause kubelet to reject the pod.
        #[arg(long)]
        chips_annotation: Option<String>,
        /// Taint key identifying nodes managed by this scheduler.
        #[arg(long, default_value = "scheduler")]
        taint_key: String,
        /// Taint value paired with `--taint-key`.
        #[arg(long, default_value = "custom")]
        taint_value: String,
        /// Path to a JSON file defining quota guarantees. Each quota is
        /// an object with `name` and `guarantees` (cluster -> chip_type -> count).
        #[arg(long)]
        quotas: Option<PathBuf>,
        /// Append each solver request as a JSON line to this file.
        #[arg(long)]
        record: Option<PathBuf>,
        /// Python solver to use (e.g. "heuristic").
        #[arg(long, default_value = "heuristic")]
        solver: String,
    },
    /// Run the scheduler service: HTTP API + binder loop.
    ///
    /// Exposes an HTTP API for workload submission (Job or Pod) and status
    /// queries, while running the binder loop in the background to place
    /// workloads across clusters.
    Serve {
        /// Log placement decisions without actually creating/patching workloads.
        #[arg(long)]
        dry_run: bool,
        /// Clusters to manage (same format as `bind`).
        #[arg(long = "cluster", required = true)]
        clusters: Vec<String>,
        /// Node label key for chip/accelerator type.
        #[arg(long, default_value = "accelerator")]
        chip_label: String,
        /// Resource name for chip/GPU capacity.
        #[arg(long, default_value = "nvidia.com/gpu")]
        chip_resource: String,
        /// If set, read per-node chip count from this node label instead of
        /// the extended resource. Used by test clusters without a device plugin.
        #[arg(long)]
        chip_count_label: Option<String>,
        /// If set, read per-replica chip count from this annotation as a
        /// fallback. See the `bind` subcommand for details.
        #[arg(long)]
        chips_annotation: Option<String>,
        /// Taint key identifying nodes managed by this scheduler.
        #[arg(long, default_value = "scheduler")]
        taint_key: String,
        /// Taint value paired with `--taint-key`.
        #[arg(long, default_value = "custom")]
        taint_value: String,
        /// Port for the HTTP API server.
        #[arg(long, default_value = "8080")]
        port: u16,
        /// Path to a JSON file defining quota guarantees.
        #[arg(long)]
        quotas: Option<PathBuf>,
        /// Append each solver request as a JSON line to this file.
        #[arg(long)]
        record: Option<PathBuf>,
        /// Python solver to use (e.g. "heuristic").
        #[arg(long, default_value = "heuristic")]
        solver: String,
        /// Postgres connection string for the durable workload store.
        /// Falls back to the `DATABASE_URL` env var.  Required: there is no
        /// in-memory fallback for `serve` because that would silently
        /// disable persistence in production.
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Observe cluster events in real time.
    Observe {
        /// Resource type to watch.
        #[arg(long, default_value = "pods")]
        resource: observer::Resource,
        /// Namespace to watch (default: all namespaces).
        #[arg(long)]
        namespace: Option<String>,
        /// Show finalizers on each object.
        #[arg(long)]
        show_finalizers: bool,
    },
}

/// Load quota definitions from a JSON file, or return an empty list if no path
/// is provided.
///
/// The file should contain a JSON array of quota objects, each with `name` and
/// `guarantees` fields matching the solver's `Quota` type.
fn load_quotas(path: Option<&std::path::Path>) -> anyhow::Result<Vec<solver_types::Quota>> {
    let Some(path) = path else {
        return Ok(vec![]);
    };
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read quotas file: {}", path.display()))?;
    let quotas: Vec<solver_types::Quota> = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse quotas file: {}", path.display()))?;
    tracing::info!(count = quotas.len(), path = %path.display(), "loaded quotas");
    Ok(quotas)
}

fn parse_cluster_specs(clusters: &[String]) -> Vec<binder::ClusterSpec> {
    clusters
        .iter()
        .map(|s| {
            if let Some((name, context)) = s.split_once(':') {
                binder::ClusterSpec {
                    name: name.to_string(),
                    context: Some(context.to_string()),
                }
            } else {
                binder::ClusterSpec {
                    name: s.clone(),
                    context: None,
                }
            }
        })
        .collect()
}

/// Initialise Sentry if `SENTRY_DSN` is set. The returned guard must outlive
/// `main` — when dropped it flushes any in-flight events. Bind it to a named
/// variable in `main` (not `_`) so its lifetime extends to process exit.
///
/// `sentry::init` registers a panic handler automatically, which is the main
/// reason this exists: without it, the bridge crashes silently in production.
fn init_sentry() -> Option<sentry::ClientInitGuard> {
    let dsn = std::env::var("SENTRY_DSN").ok().filter(|s| !s.is_empty())?;
    let guard = sentry::init((
        dsn,
        sentry::ClientOptions {
            release: std::env::var("GIT_SHA").ok().map(Into::into),
            environment: Some(
                std::env::var("SENTRY_ENV")
                    .unwrap_or_else(|_| "production".into())
                    .into(),
            ),
            traces_sample_rate: 1.0,
            ..Default::default()
        },
    ));
    Some(guard)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise Sentry first so its panic hook is in place before any other
    // setup runs. The guard binds for the whole process lifetime.
    let _sentry_guard = init_sentry();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Bind {
            dry_run,
            clusters,
            chip_label,
            chip_resource,
            chip_count_label,
            chips_annotation,
            taint_key,
            taint_value,
            quotas,
            record,
            solver,
        } => {
            let cluster_specs = parse_cluster_specs(&clusters);
            let loaded_quotas = load_quotas(quotas.as_deref())?;
            let config = binder::BinderConfig {
                chip_label,
                chip_resource,
                chip_count_label,
                chips_annotation,
                taint_key,
                taint_value,
                quotas: loaded_quotas,
                solver_name: solver,
                ..binder::BinderConfig::default()
            };
            binder::run(dry_run, &cluster_specs, &config, None, None, None, record).await
        }
        Command::Serve {
            dry_run,
            clusters,
            chip_label,
            chip_resource,
            chip_count_label,
            chips_annotation,
            taint_key,
            taint_value,
            port,
            quotas,
            record,
            solver,
            database_url,
        } => {
            let cluster_specs = parse_cluster_specs(&clusters);
            let loaded_quotas = load_quotas(quotas.as_deref())?;
            let config = binder::BinderConfig {
                chip_label,
                chip_resource,
                chip_count_label,
                chips_annotation,
                taint_key,
                taint_value,
                quotas: loaded_quotas,
                solver_name: solver,
                ..binder::BinderConfig::default()
            };

            let pg_store = persistence::PgStore::connect(&database_url).await?;
            let store = job_store::WorkloadStore::new(std::sync::Arc::new(pg_store)).await?;
            let scheduler_state = job_store::new_scheduler_state();
            let snapshot_state = snapshot::new_snapshot_state();

            let app = api::router(
                store.clone(),
                scheduler_state.clone(),
                snapshot_state.clone(),
            );
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            tracing::info!(port, "HTTP API listening");

            tokio::select! {
                res = axum::serve(listener, app) => {
                    res.map_err(|e| anyhow::anyhow!("HTTP server error: {e}"))
                }
                res = binder::run(
                    dry_run,
                    &cluster_specs,
                    &config,
                    Some(store),
                    Some(scheduler_state),
                    Some(snapshot_state),
                    record,
                ) => {
                    res
                }
            }
        }
        Command::Observe {
            resource,
            namespace,
            show_finalizers,
        } => observer::run(resource, namespace, show_finalizers).await,
    }
}
