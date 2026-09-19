//! Private, versioned public-data restart caches; never a readiness authority.
use crate::{
    model::{Candle, MAX_HISTORY, now, valid_pair, validate_candle},
    state::Shared,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
const MAX_BYTES: u64 = 8_000_000;
const MAX_AGE: f64 = 86_400.0;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    v: u8,
    source: String,
    pair: String,
    interval: String,
    capacity: usize,
    saved_at: f64,
    rows: Vec<Candle>,
    sha256: String,
}
#[derive(Clone)]
pub struct Cache {
    dir: PathBuf,
}
impl Cache {
    pub fn open(dir: &Path) -> Result<Self> {
        if !dir.exists() {
            fs::DirBuilder::new().recursive(true).create(dir)?;
            fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        }
        let meta = fs::symlink_metadata(dir)?;
        ensure!(
            meta.is_dir() && meta.permissions().mode() & 0o777 == 0o700,
            "cache directory must be a non-symlink mode0700 directory"
        );
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }
    fn path(&self, pair: &str) -> Result<PathBuf> {
        ensure!(valid_pair(pair), "invalid cache pair");
        Ok(self.dir.join(format!("{pair}.json")))
    }
    pub fn load(&self, pair: &str, capacity: usize, at: f64) -> Result<Option<Vec<Candle>>> {
        let path = self.path(pair)?;
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
        {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let meta = file.metadata()?;
        ensure!(
            meta.is_file() && meta.len() <= MAX_BYTES && meta.permissions().mode() & 0o777 == 0o600,
            "invalid cache file type, size or mode"
        );
        let mut bytes = Vec::new();
        file.take(MAX_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(bytes.len() as u64 <= MAX_BYTES, "oversized cache");
        let record: Record = serde_json::from_slice(&bytes).context("invalid cache JSON")?;
        ensure!(
            record.v == 1
                && record.source == "okx-public-spot"
                && record.interval == "1m"
                && record.pair == pair,
            "cache provenance mismatch"
        );
        ensure!(
            record.saved_at.is_finite() && record.saved_at <= at && at - record.saved_at <= MAX_AGE,
            "cache age invalid"
        );
        ensure!(
            (capacity..=MAX_HISTORY).contains(&record.capacity)
                && record.rows.len() == record.capacity,
            "cache history partial or insufficient"
        );
        ensure!(
            digest(&record.rows)? == record.sha256,
            "cache checksum mismatch"
        );
        ensure!(
            record
                .rows
                .windows(2)
                .all(|w| w[1].open_time == w[0].open_time + 60),
            "cache history gap"
        );
        for row in &record.rows {
            validate_candle(row, record.saved_at)?;
        }
        ensure!(
            record
                .rows
                .last()
                .is_some_and(|r| at - (r.open_time + 60) as f64 <= MAX_AGE),
            "cache history too old"
        );
        Ok(Some(
            record
                .rows
                .into_iter()
                .rev()
                .take(capacity)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect(),
        ))
    }
    pub fn save(&self, pair: &str, rows: &[Candle]) -> Result<()> {
        ensure!(
            (2..=MAX_HISTORY).contains(&rows.len()),
            "cache history outside bounds"
        );
        let saved_at = now();
        ensure!(
            crate::model::history_valid(rows, rows.len(), saved_at),
            "refuse stale or partial cache write"
        );
        for row in rows {
            validate_candle(row, saved_at)?;
        }
        let record = Record {
            v: 1,
            source: "okx-public-spot".into(),
            pair: pair.into(),
            interval: "1m".into(),
            capacity: rows.len(),
            saved_at,
            sha256: digest(rows)?,
            rows: rows.to_vec(),
        };
        let bytes = serde_json::to_vec(&record)?;
        ensure!(bytes.len() as u64 <= MAX_BYTES, "cache too large");
        let target = self.path(pair)?;
        let temporary = self
            .dir
            .join(format!(".{pair}.{}.tmp", uuid::Uuid::new_v4()));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary, target)?;
            File::open(&self.dir)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temporary);
        }
        result
    }
    pub async fn load_into(&self, state: &Shared) -> Result<()> {
        let (pairs, capacity) = {
            let s = state.lock().await;
            (s.pairs.keys().cloned().collect::<Vec<_>>(), s.capacity)
        };
        for pair in pairs {
            let cache = self.clone();
            let name = pair.clone();
            let result =
                tokio::task::spawn_blocking(move || cache.load(&name, capacity, now())).await?;
            let mut s = state.lock().await;
            match result {
                Ok(Some(rows)) => {
                    s.set_seed_base(&pair, rows);
                    s.count("cache_loaded");
                }
                Ok(None) => s.count("cache_missing"),
                Err(error) => {
                    eprintln!("cache ignored {pair}: {error:#}");
                    s.count("cache_rejected");
                }
            }
        }
        Ok(())
    }
    pub async fn run(self, state: Shared, stop: CancellationToken) {
        let mut saved = BTreeMap::new();
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {_=stop.cancelled()=>break,_=tick.tick()=>{}}
            self.flush(&state, &mut saved, false).await;
        }
        self.flush(&state, &mut saved, true).await;
    }
    async fn flush(&self, state: &Shared, saved: &mut BTreeMap<String, u64>, final_flush: bool) {
        let pairs = { state.lock().await.pairs.keys().cloned().collect::<Vec<_>>() };
        for pair in pairs {
            let pending = {
                let s = state.lock().await;
                let p = &s.pairs[&pair];
                if saved.get(&pair) == Some(&p.sequence) {
                    None
                } else if p.ready {
                    Some((
                        p.sequence,
                        p.rows
                            .iter()
                            .map(|r| r.as_ref().clone())
                            .collect::<Vec<_>>(),
                    ))
                } else if final_flush
                    && p.seed_base.len() == s.capacity
                    && crate::model::history_refs_valid(
                        p.seed_base.iter().map(|r| r.as_ref()),
                        s.capacity,
                        now(),
                    )
                {
                    // Collector disconnect retains the last complete validated history.
                    // Persisting it does not restore offline readiness; save() revalidates.
                    Some((
                        p.sequence,
                        p.seed_base
                            .iter()
                            .map(|r| r.as_ref().clone())
                            .collect::<Vec<_>>(),
                    ))
                } else {
                    None
                }
            };
            let Some((sequence, rows)) = pending else {
                continue;
            };
            let cache = self.clone();
            let name = pair.clone();
            match tokio::task::spawn_blocking(move || cache.save(&name, &rows)).await {
                Ok(Ok(())) => {
                    saved.insert(pair, sequence);
                    state.lock().await.count("cache_writes");
                }
                error => {
                    eprintln!("cache write failed {pair}: {error:?}");
                    state.lock().await.count("cache_write_failures");
                }
            }
        }
    }
}
fn digest(rows: &[Candle]) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(rows)?)))
}
