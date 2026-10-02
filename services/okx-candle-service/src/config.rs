use crate::{
    auth::{Auth, Consumer},
    model::valid_pair,
};
use anyhow::{Context, Result, ensure};
use clap::Parser;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    net::SocketAddr,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

#[derive(Parser, Debug)]
#[command(version, about = "Public OKX confirmed 1m USDC candle service")]
pub struct Args {
    #[arg(long)]
    pub universe: PathBuf,
    /// Legacy single full-scope bearer token. Optional when `--consumers-file` is given.
    #[arg(long)]
    pub token_file: Option<PathBuf>,
    /// Consumer-scoped tokens: JSON `{"consumers":[{"id","token","pairs","max_history","max_streams"}]}`,
    /// regular mode-0600 file. Each consumer sees only its pairs and history ceiling.
    #[arg(long)]
    pub consumers_file: Option<PathBuf>,
    #[arg(long, default_value = "127.0.0.1:8766")]
    pub listen: SocketAddr,
    #[arg(long, default_value_t = 90)]
    pub history: usize,
    /// Optional private directory for validated public-candle restart caches.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
    /// Optional absolute path of a mode-0600 JSON status file (the `/v1/metrics` view, 60-bar
    /// digest window) rewritten atomically every five seconds for host-side monitors.
    #[arg(long)]
    pub status_file: Option<PathBuf>,
    /// Enable the separately versioned, bounded Market Picture history projection.
    #[arg(long, default_value_t = false)]
    pub market_picture: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Universe {
    pairs: Vec<String>,
}

impl Args {
    pub fn validate(&self) -> Result<(Vec<String>, Auth)> {
        ensure!(
            self.listen.ip() == std::net::Ipv4Addr::LOCALHOST,
            "listen must be 127.0.0.1"
        );
        ensure!(
            (2..=crate::model::MAX_HISTORY).contains(&self.history),
            "history must be 2..10000"
        );
        ensure!(
            self.token_file.is_some() || self.consumers_file.is_some(),
            "provide --token-file and/or --consumers-file"
        );
        let legacy = self
            .token_file
            .as_deref()
            .map(|path| {
                read_secret_file(path, 256)
                    .and_then(|text| parse_token(text.trim_end_matches(['\r', '\n'])))
            })
            .transpose()?;
        ensure!(
            fs::metadata(&self.universe)?.len() <= 128_000,
            "universe too large"
        );
        let u: Universe = serde_json::from_slice(&fs::read(&self.universe)?)?;
        ensure!(
            !u.pairs.is_empty() && u.pairs.len() <= 2000,
            "universe must contain 1..2000 pairs"
        );
        ensure!(
            u.pairs.len().saturating_mul(self.history) <= 3_000_000,
            "configured histories exceed 3000000-row process budget"
        );
        if self.market_picture {
            ensure!(
                self.cache_dir.is_some(),
                "market picture history requires the existing private cache directory"
            );
            ensure!(
                u.pairs.len() <= crate::market_picture::MAX_MARKET_PICTURE_PAIRS,
                "market picture is limited to 300 configured pairs"
            );
            let extension_bytes =
                crate::market_picture::estimated_extension_memory_bytes(u.pairs.len())
                    .context("market picture memory estimate overflow")?;
            ensure!(
                extension_bytes <= crate::market_picture::MAX_EXTENSION_MEMORY_BYTES,
                "market picture projection exceeds its 1 GiB extension memory budget"
            );
        }
        ensure!(
            u.pairs.iter().all(|p| valid_pair(p)),
            "invalid USDC spot pair"
        );
        ensure!(
            u.pairs.iter().collect::<BTreeSet<_>>().len() == u.pairs.len(),
            "duplicate pair"
        );
        if let Some(path) = &self.status_file {
            crate::status::validate(path)?;
        }
        let mut consumers = Vec::new();
        if let Some(token) = legacy {
            consumers.push(Consumer::new(
                "default",
                token,
                None,
                self.history,
                crate::server::GLOBAL_STREAMS,
            ));
        }
        if let Some(path) = &self.consumers_file {
            consumers.extend(self.read_consumers(path, &u.pairs)?);
        }
        let tokens: BTreeSet<_> = consumers.iter().map(|c| c.token().to_string()).collect();
        ensure!(tokens.len() == consumers.len(), "duplicate consumer token");
        let ids: BTreeSet<_> = consumers.iter().map(|c| c.id.clone()).collect();
        ensure!(ids.len() == consumers.len(), "duplicate consumer id");
        Ok((u.pairs, Auth::new(consumers)))
    }

    fn read_consumers(&self, path: &std::path::Path, catalog: &[String]) -> Result<Vec<Consumer>> {
        let file: ConsumerFile = serde_json::from_str(&read_secret_file(path, 16_384)?)
            .context("consumers file must be valid JSON of the documented shape")?;
        ensure!(
            (1..=16).contains(&file.consumers.len()),
            "consumers file must define 1..16 consumers"
        );
        let catalog: BTreeSet<_> = catalog.iter().cloned().collect();
        let mut out = Vec::new();
        for entry in file.consumers {
            ensure!(
                !entry.id.is_empty()
                    && entry.id.len() <= 32
                    && entry
                        .id
                        .bytes()
                        .next()
                        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    && entry.id.bytes().all(|b| b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || b == b'-'
                        || b == b'_'),
                "consumer id must be 1..32 chars of [a-z0-9_-]"
            );
            let token = parse_token(&entry.token)?;
            ensure!(
                !entry.pairs.is_empty(),
                "consumer {} has no pairs",
                entry.id
            );
            let scope: BTreeSet<_> = entry.pairs.iter().cloned().collect();
            ensure!(
                scope.len() == entry.pairs.len(),
                "consumer {} repeats a pair",
                entry.id
            );
            ensure!(
                scope.is_subset(&catalog),
                "consumer {} requests a pair outside the universe",
                entry.id
            );
            ensure!(
                (2..=self.history).contains(&entry.max_history),
                "consumer {} max_history must be 2..=service history {}",
                entry.id,
                self.history
            );
            ensure!(
                (1..=crate::server::GLOBAL_STREAMS).contains(&entry.max_streams),
                "consumer {} max_streams must be 1..=32",
                entry.id
            );
            out.push(Consumer::new(
                entry.id,
                token,
                Some(scope),
                entry.max_history,
                entry.max_streams,
            ));
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerFile {
    consumers: Vec<ConsumerEntry>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerEntry {
    id: String,
    token: String,
    pairs: Vec<String>,
    max_history: usize,
    max_streams: usize,
}

fn parse_token(token: &str) -> Result<String> {
    ensure!(
        (32..=128).contains(&token.len())
            && token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
        "token must be 32..128 URL-safe characters"
    );
    Ok(token.to_string())
}

/// Read a secret file only if it is a non-symlink regular mode-0600 file of bounded size.
/// Contents are returned to the caller and never logged here.
fn read_secret_file(path: &std::path::Path, max_len: u64) -> Result<String> {
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_file() && meta.permissions().mode() & 0o777 == 0o600 && meta.len() <= max_len,
        "secret file must be a regular mode0600 file <={max_len} bytes"
    );
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .context("open secret file")?;
    let opened = file.metadata()?;
    ensure!(
        opened.ino() == meta.ino()
            && opened.dev() == meta.dev()
            && opened.permissions().mode() & 0o777 == 0o600,
        "secret file changed during open"
    );
    let mut text = String::new();
    file.take(max_len + 1).read_to_string(&mut text)?;
    ensure!(text.len() as u64 <= max_len, "secret file grew during read");
    Ok(text)
}
