use crate::model::valid_pair;
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
    #[arg(long)]
    pub token_file: PathBuf,
    #[arg(long, default_value = "127.0.0.1:8766")]
    pub listen: SocketAddr,
    #[arg(long, default_value_t = 90)]
    pub history: usize,
    /// Optional private directory for validated public-candle restart caches.
    #[arg(long)]
    pub cache_dir: Option<PathBuf>,
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
    pub fn validate(&self) -> Result<(Vec<String>, String)> {
        ensure!(
            self.listen.ip() == std::net::Ipv4Addr::LOCALHOST,
            "listen must be 127.0.0.1"
        );
        ensure!(
            (2..=crate::model::MAX_HISTORY).contains(&self.history),
            "history must be 2..10000"
        );
        let meta = fs::symlink_metadata(&self.token_file)?;
        ensure!(
            meta.is_file() && meta.permissions().mode() & 0o777 == 0o600 && meta.len() <= 256,
            "token file must be a regular mode0600 file <=256 bytes"
        );
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&self.token_file)
            .context("open token")?;
        let opened = file.metadata()?;
        ensure!(
            opened.ino() == meta.ino()
                && opened.dev() == meta.dev()
                && opened.permissions().mode() & 0o777 == 0o600,
            "token file changed during open"
        );
        let mut token = String::new();
        file.take(257).read_to_string(&mut token)?;
        ensure!(token.len() <= 256, "token file grew during read");
        let token = token.trim_end_matches(['\r', '\n']).to_string();
        ensure!(
            (32..=128).contains(&token.len())
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "token must be 32..128 URL-safe characters"
        );
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
                u.pairs.len().saturating_mul(
                    self.history
                        + crate::market_picture::MINUTE_ROWS
                        + crate::market_picture::HOUR_ROWS
                        + crate::market_picture::DAY_ROWS
                ) <= 3_000_000,
                "market picture and candle histories exceed 3000000-row process budget"
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
        Ok((u.pairs, token))
    }
}
