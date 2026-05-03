//! Postgres implementation of [`crate::job_store::Persistence`].
//!
//! Schema (a single table — workloads serialise to JSON):
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS workloads (
//!     name        TEXT PRIMARY KEY,
//!     blob        JSONB NOT NULL,
//!     updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
//! );
//! ```
//!
//! No schema-version column for v0.  When the [`crate::job_store::Workload`]
//! shape changes, deploy a migration alongside the code.  Old rows that fail
//! to deserialise are dropped on startup with a warning — the cluster
//! reflectors will rediscover any actually-running workload, so a stale row
//! is recoverable.

use anyhow::{Context, Result};
use async_trait::async_trait;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, PgPool, Row};
use std::str::FromStr;
use std::time::Duration;

use crate::job_store::{Persistence, Workload};

/// Postgres-backed persistence.
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Connect, ensure the schema exists, and return a handle.  Retries the
    /// initial connection so a fresh stack (Postgres still booting next to
    /// the scheduler) doesn't crash-loop the bridge.
    pub async fn connect(database_url: &str) -> Result<Self> {
        // Lower the sqlx INFO logging spam to debug — every query at INFO
        // would drown out the binder logs.
        let opts = PgConnectOptions::from_str(database_url)
            .context("parsing DATABASE_URL")?
            .log_statements(tracing::log::LevelFilter::Debug);

        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts)
            .await
            .context("connecting to Postgres")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workloads (
                name        TEXT PRIMARY KEY,
                blob        JSONB NOT NULL,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&pool)
        .await
        .context("ensuring workloads table")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl Persistence for PgStore {
    async fn load_all(&self) -> Result<Vec<(String, Workload)>> {
        let rows = sqlx::query("SELECT name, blob FROM workloads")
            .fetch_all(&self.pool)
            .await
            .context("loading workloads")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get("name");
            let blob: serde_json::Value = row.get("blob");
            match serde_json::from_value::<Workload>(blob) {
                Ok(w) => out.push((name, w)),
                Err(e) => {
                    // Drop the row rather than crash on schema drift;
                    // reflectors will rediscover any in-flight workload.
                    tracing::warn!(workload = %name, error = %e, "dropping unreadable workload row");
                }
            }
        }
        Ok(out)
    }

    async fn upsert(&self, name: &str, workload: &Workload) -> Result<()> {
        let blob = serde_json::to_value(workload).context("serialising workload")?;
        sqlx::query(
            r#"
            INSERT INTO workloads (name, blob, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (name) DO UPDATE
              SET blob = EXCLUDED.blob,
                  updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(name)
        .bind(blob)
        .execute(&self.pool)
        .await
        .with_context(|| format!("upserting workload '{name}'"))?;
        Ok(())
    }

    async fn remove(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM workloads WHERE name = $1")
            .bind(name)
            .execute(&self.pool)
            .await
            .with_context(|| format!("removing workload '{name}'"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    //! Integration tests against a real Postgres.  Skipped unless
    //! `DATABASE_URL` is set so `cargo test` doesn't require Docker
    //! by default.  Run with:
    //!
    //!   DATABASE_URL=postgres://scheduler:scheduler@localhost:5432/scheduler \
    //!     cargo test -p k8s-bridge --bin k8s-bridge -- --ignored
    use super::*;
    use crate::job_store::{ManagedObject, Persistence, Workload};
    use k8s_openapi::api::batch::v1::Job;

    fn workload(name: &str) -> Workload {
        let job = Job {
            metadata: kube::core::ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            ..Default::default()
        };
        Workload {
            managed: ManagedObject::Job(Box::new(job)),
            generation: 0,
            consecutive_failures: 0,
        }
    }

    #[tokio::test]
    #[ignore = "requires DATABASE_URL pointing at a Postgres"]
    async fn pg_store_upsert_load_remove() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
        let store = PgStore::connect(&url).await.unwrap();
        // Use a name no other test would pick.
        let name = "ittest-pg-roundtrip";
        // Clean from any previous run.
        store.remove(name).await.unwrap();

        store.upsert(name, &workload(name)).await.unwrap();
        let rows = store.load_all().await.unwrap();
        assert!(rows.iter().any(|(n, _)| n == name));

        store.remove(name).await.unwrap();
        let rows = store.load_all().await.unwrap();
        assert!(!rows.iter().any(|(n, _)| n == name));
    }
}
