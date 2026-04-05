//! Toy Kubernetes cluster observer.
//!
//! Watches pod and node events from a Kubernetes cluster and prints them.
//! Uses kube-rs watch streams with automatic reconnection.

use anyhow::{Context, Result};
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Client, ResourceExt,
    api::Api,
    runtime::{WatchStreamExt, watcher},
};
use tracing::info;

/// Which resource type to observe.
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum Resource {
    Pods,
    Nodes,
}

/// Run the observer, streaming events for the chosen resource.
pub async fn run(
    resource: Resource,
    namespace: Option<String>,
    show_finalizers: bool,
) -> Result<()> {
    let client = Client::try_default()
        .await
        .context("failed to create kube client")?;

    info!("watching {resource:?}...");

    match resource {
        Resource::Pods => watch_pods(client, namespace, show_finalizers).await,
        Resource::Nodes => watch_nodes(client, show_finalizers).await,
    }
}

async fn watch_pods(
    client: Client,
    namespace: Option<String>,
    show_finalizers: bool,
) -> Result<()> {
    let api: Api<Pod> = match &namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    };

    let stream = watcher(api, watcher::Config::default()).default_backoff();
    futures::pin_mut!(stream);

    while let Some(event) = stream.try_next().await? {
        match event {
            watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
                let name = pod.name_any();
                let ns = pod.namespace().unwrap_or_else(|| "-".into());
                let phase = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("Unknown");
                let node = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.node_name.as_deref())
                    .unwrap_or("<unscheduled>");
                let scheduler = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.scheduler_name.as_deref())
                    .unwrap_or("default-scheduler");

                let mut line = format!(
                    "pod/{name}  ns={ns}  phase={phase}  node={node}  scheduler={scheduler}"
                );

                // Resource requests.
                if let Some(spec) = &pod.spec {
                    for c in &spec.containers {
                        if let Some(res) = &c.resources
                            && let Some(req) = &res.requests
                        {
                            let parts: Vec<String> =
                                req.iter().map(|(k, v)| format!("{k}={}", v.0)).collect();
                            if !parts.is_empty() {
                                line.push_str(&format!(
                                    "  container={}  requests=[{}]",
                                    c.name,
                                    parts.join(", ")
                                ));
                            }
                        }
                    }
                }

                if show_finalizers
                    && let Some(fins) = &pod.metadata.finalizers
                    && !fins.is_empty()
                {
                    line.push_str(&format!("  finalizers={fins:?}"));
                }

                println!("{line}");
            }
            watcher::Event::Delete(pod) => {
                println!("DELETED  pod/{}", pod.name_any());
            }
            watcher::Event::Init | watcher::Event::InitDone => {}
        }
    }

    Ok(())
}

async fn watch_nodes(client: Client, show_finalizers: bool) -> Result<()> {
    let api: Api<Node> = Api::all(client);

    let stream = watcher(api, watcher::Config::default()).default_backoff();
    futures::pin_mut!(stream);

    while let Some(event) = stream.try_next().await? {
        match event {
            watcher::Event::Apply(node) | watcher::Event::InitApply(node) => {
                let name = node.name_any();
                let labels = node.labels();
                let accel = labels
                    .get("accelerator")
                    .map(|s| s.as_str())
                    .unwrap_or("none");

                let (cpu, mem, ready) = node
                    .status
                    .as_ref()
                    .map(|s| {
                        let alloc = s.allocatable.as_ref();
                        let cpu = alloc
                            .and_then(|a| a.get("cpu"))
                            .map(|v| v.0.clone())
                            .unwrap_or_else(|| "?".into());
                        let mem = alloc
                            .and_then(|a| a.get("memory"))
                            .map(|v| v.0.clone())
                            .unwrap_or_else(|| "?".into());
                        let ready = s
                            .conditions
                            .as_ref()
                            .and_then(|cs| cs.iter().find(|c| c.type_ == "Ready"))
                            .map(|c| c.status.as_str())
                            .unwrap_or("Unknown");
                        (cpu, mem, ready.to_string())
                    })
                    .unwrap_or_else(|| ("?".into(), "?".into(), "Unknown".into()));

                let mut line = format!(
                    "node/{name}  ready={ready}  accelerator={accel}  cpu={cpu}  mem={mem}"
                );

                if show_finalizers
                    && let Some(fins) = &node.metadata.finalizers
                    && !fins.is_empty()
                {
                    line.push_str(&format!("  finalizers={fins:?}"));
                }

                println!("{line}");
            }
            watcher::Event::Delete(node) => {
                println!("DELETED  node/{}", node.name_any());
            }
            watcher::Event::Init | watcher::Event::InitDone => {}
        }
    }

    Ok(())
}
