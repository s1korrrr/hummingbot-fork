//! Optional periodic status file for host-side monitors. The service listens on a loopback
//! address that is only reachable inside its network namespace, so alerting processes on the
//! host read this file (the `/v1/metrics` view) instead of opening a socket.
use crate::state::Shared;
use anyhow::{Context, Result, ensure};
use std::{
    fs,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub const WINDOW: usize = 60;
pub const INTERVAL: Duration = Duration::from_secs(5);

/// The parent directory must exist and the target must not be a symlink or non-regular file.
pub fn validate(path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "status file must be an absolute path");
    let parent = path
        .parent()
        .context("status file needs a parent directory")?;
    ensure!(parent.is_dir(), "status file parent directory must exist");
    match fs::symlink_metadata(path) {
        Ok(meta) => ensure!(meta.is_file(), "status file target must be a regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Atomic same-directory replace with mode 0600; readers never observe a partial file.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let temporary: PathBuf = path.with_extension(format!("tmp-{}", std::process::id()));
    let result = (|| -> Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub async fn run(state: Shared, path: PathBuf, stop: CancellationToken) {
    let mut tick = tokio::time::interval(INTERVAL);
    loop {
        tokio::select! { _ = stop.cancelled() => break, _ = tick.tick() => {} }
        let value = state
            .lock()
            .await
            .metrics(None, WINDOW, crate::model::now());
        let result = serde_json::to_vec(&value)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| {
                let path = path.clone();
                write_atomic(&path, &bytes)
            });
        if let Err(error) = result {
            eprintln!("status file write failed: {error:#}");
            state.lock().await.count("status_write_failures");
        }
    }
}
