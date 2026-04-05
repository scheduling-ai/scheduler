//! Solver subprocess: serializes cluster state to JSON, calls the Python
//! solver via stdin/stdout, and returns the parsed result.

use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::solver_types::{ScheduleResult, SolverRequest};

/// Hard upper bound on solver wall-clock time.  If the subprocess doesn't
/// finish within this window we kill it and skip the cycle.
const SOLVER_TIMEOUT: Duration = Duration::from_secs(60);

/// Call the Python solver as a subprocess.
///
/// Sends `request` as JSON on stdin, reads `ScheduleResult` JSON from stdout.
/// The subprocess is killed if it exceeds [`SOLVER_TIMEOUT`].
///
/// If `record_path` is provided, the serialized request is appended as a
/// single JSON line to that file before invoking the solver.
pub async fn call_solver(
    request: &SolverRequest,
    record_path: Option<&std::path::Path>,
    solver_name: &str,
) -> Result<ScheduleResult> {
    let request_json =
        serde_json::to_string(request).context("failed to serialize SolverRequest")?;
    debug!(bytes = request_json.len(), "sending solver request");

    if let Some(path) = record_path {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .context("failed to open session record file")?;
        writeln!(f, "{request_json}").context("failed to write to session record file")?;
    }

    let mut child = Command::new("uv")
        .args(["run", "--no-sync", "python", "-m", "scheduler", solver_name])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn solver process")?;

    let mut stdin = child.stdin.take().context("failed to open solver stdin")?;
    stdin
        .write_all(request_json.as_bytes())
        .await
        .context("failed to write to solver stdin")?;
    drop(stdin);

    let wait_fut = async {
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();

        let mut stdout = child.stdout.take().context("missing stdout")?;
        let mut stderr = child.stderr.take().context("missing stderr")?;

        let (_, _, status) = tokio::try_join!(
            async {
                tokio::io::AsyncReadExt::read_to_end(&mut stdout, &mut stdout_buf)
                    .await
                    .context("read stdout")
            },
            async {
                tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut stderr_buf)
                    .await
                    .context("read stderr")
            },
            async { child.wait().await.context("wait") },
        )?;

        Ok::<_, anyhow::Error>((status, stdout_buf, stderr_buf))
    };

    let (status, stdout_buf, stderr_buf) =
        match tokio::time::timeout(SOLVER_TIMEOUT, wait_fut).await {
            Ok(result) => result?,
            Err(_) => {
                warn!(
                    timeout_secs = SOLVER_TIMEOUT.as_secs(),
                    "solver timed out, killing subprocess"
                );
                // child is still alive — kill it.
                let _ = child.kill().await;
                anyhow::bail!("solver timed out after {}s", SOLVER_TIMEOUT.as_secs());
            }
        };

    if !status.success() {
        let stderr = String::from_utf8_lossy(&stderr_buf);
        anyhow::bail!("solver exited with {}: {}", status, stderr);
    }

    let result: ScheduleResult =
        serde_json::from_slice(&stdout_buf).context("failed to parse solver response JSON")?;

    info!(
        pods = result.pods.len(),
        status = %result.solver_status,
        "solver returned"
    );

    Ok(result)
}
