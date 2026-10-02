//! Optional bounded, source-owned history projection for Market Picture.
//!
//! This projection is deliberately separate from wire-v1 history capacity and
//! readiness. It consumes only confirmed rows already admitted by this owner.

use crate::{
    model::{Candle, now},
    state::{Event, Shared},
};
use anyhow::{Context, Result, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    sync::Mutex,
    time::{Duration, interval},
};
use tokio_util::sync::CancellationToken;

pub const SCHEMA_VERSION: &str = "market-picture.source-history.v1";
pub const MINUTE_ROWS: usize = 11_521;
pub const HOUR_ROWS: usize = 3_001;
pub const DAY_ROWS: usize = 401;
pub const MAX_RESPONSE_BYTES: usize = 8_000_000;
pub const MAX_UPDATE_RESPONSE_BYTES: usize = 64_000;
pub const MAX_MARKET_PICTURE_PAIRS: usize = 300;
pub const MAX_EXTENSION_MEMORY_BYTES: u64 = 1_073_741_824;
const MAX_CACHE_BYTES: u64 = 8_000_000;
const MAX_CACHE_TOTAL_BYTES: u64 = 2_000_000_000;
const MAX_CACHED_PRE_CLOSE_SKEW_MS: u64 = 2_000;
const MAX_UPDATE_JOURNAL_BYTES: usize = 16_384;
const MAX_UPDATE_BATCHES: usize = 128;
const MAX_UPDATE_ROWS: usize = 128;

/// Conservative reserved-buffer estimate for the optional projection. VecDeque
/// buffers are pre-sized; 32 extra bytes per slot cover allocator/slab overhead
/// and conservatively account for shared source-epoch provenance allocations.
pub fn estimated_extension_memory_bytes(pair_count: usize) -> Option<u64> {
    if pair_count > MAX_MARKET_PICTURE_PAIRS {
        return None;
    }
    let rows_per_pair = MINUTE_ROWS + HOUR_ROWS + DAY_ROWS;
    let row_bytes = std::mem::size_of::<SourceBar>().checked_add(32)?;
    let row_storage = pair_count
        .checked_mul(rows_per_pair)?
        .checked_mul(row_bytes)?;
    let journal_reserve =
        3usize.checked_mul(MAX_UPDATE_JOURNAL_BYTES.checked_mul(2)?.checked_add(1024)?)?;
    let pair_overhead = pair_count.checked_mul(
        4096usize
            .checked_add(std::mem::size_of::<PairHistory>())?
            .checked_add(journal_reserve)?,
    )?;
    u64::try_from(row_storage.checked_add(pair_overhead)?).ok()
}

fn canonical_decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_canonical_decimal(value: &str) -> Result<Decimal> {
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'.'),
        "source decimal is not canonical"
    );
    let parsed = Decimal::from_str_exact(value).context("invalid decimal source value")?;
    ensure!(
        canonical_decimal_text(parsed) == value,
        "source decimal is not canonical"
    );
    Ok(parsed)
}

mod canonical_decimal {
    use super::{Decimal, canonical_decimal_text, parse_canonical_decimal};
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_canonical_decimal(&value).map_err(D::Error::custom)
    }

    pub fn serialize<S>(value: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&canonical_decimal_text(*value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Interval {
    Minute,
    Hour,
    Day,
}
impl Interval {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "1m" => Some(Self::Minute),
            "1h" => Some(Self::Hour),
            "1d" => Some(Self::Day),
            _ => None,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Minute => "1m",
            Self::Hour => "1h",
            Self::Day => "1d",
        }
    }
    pub fn seconds(self) -> u64 {
        match self {
            Self::Minute => 60,
            Self::Hour => 3_600,
            Self::Day => 86_400,
        }
    }
    pub fn expected_rows(self) -> usize {
        match self {
            Self::Minute => MINUTE_ROWS,
            Self::Hour => HOUR_ROWS,
            Self::Day => DAY_ROWS,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceBar {
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    #[serde(with = "canonical_decimal")]
    pub open: Decimal,
    #[serde(with = "canonical_decimal")]
    pub high: Decimal,
    #[serde(with = "canonical_decimal")]
    pub low: Decimal,
    #[serde(with = "canonical_decimal")]
    pub close: Decimal,
    #[serde(with = "canonical_decimal")]
    pub volume: Decimal,
    #[serde(with = "canonical_decimal")]
    pub quote_volume: Decimal,
    pub confirmed: bool,
    pub available_at_ms: u64,
    pub source_epoch: Arc<str>,
    pub source_sequence: u64,
}

struct SourceObservation {
    pair: String,
    source_epoch: Arc<str>,
    source_sequence: u64,
    published_at_ms: u64,
    received_at_ms: u64,
    bars: Vec<SourceBar>,
    snapshot: bool,
    now_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub expected_count: usize,
    pub valid_count: usize,
    pub gap_count: usize,
    pub first_open_time_ms: Option<u64>,
    pub last_close_time_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregateInvalidReason {
    VolumeOverflow,
    QuoteVolumeOverflow,
}

enum AggregateOutcome {
    Incomplete,
    Invalid(AggregateInvalidReason),
    Complete(SourceBar),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryResponse {
    pub schema_version: String,
    pub encoding: String,
    pub source: SourceIdentity,
    pub pair: String,
    pub interval: String,
    pub epoch: String,
    pub sequence: u64,
    pub source_epoch: String,
    pub source_sequence: u64,
    pub source_epochs: Vec<String>,
    pub published_at_ms: u64,
    pub available_at_ms: u64,
    pub expires_at_ms: u64,
    /// Exclusive close boundary of the newest returned bar.
    pub cutoff_ms: Option<u64>,
    pub coverage: Coverage,
    pub ready: bool,
    pub reason: Option<String>,
    pub bars: Vec<WireBar>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpdateBar {
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: String,
    pub confirmed: bool,
    pub available_at_ms: u64,
    pub source_epoch: String,
    pub source_sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdatesResponse {
    pub mode: String,
    pub from_sequence: Option<u64>,
    #[serde(flatten)]
    pub history: HistoryResponse,
    pub upserts: Vec<UpdateBar>,
    pub deleted_open_time_ms: Vec<u64>,
}

#[derive(Clone, Debug)]
struct UpdateBatch {
    from: u64,
    to: u64,
    upserts: BTreeMap<u64, SourceBar>,
    deleted: std::collections::BTreeSet<u64>,
    bytes: usize,
}

#[derive(Clone, Debug, Default)]
struct UpdateJournal {
    sequence: u64,
    surface: Option<(u64, u64)>,
    batches: VecDeque<UpdateBatch>,
    bytes: usize,
    force_full: bool,
}

impl UpdateJournal {
    fn reset_at(&mut self, sequence: u64) {
        self.sequence = sequence;
        self.batches.clear();
        self.bytes = 0;
        self.force_full = false;
    }

    fn reset_after_change(&mut self) {
        if let Some(next) = self.sequence.checked_add(1) {
            self.reset_at(next);
        } else {
            self.batches.clear();
            self.bytes = 0;
            self.force_full = true;
        }
    }

    fn append(
        &mut self,
        upserts: BTreeMap<u64, SourceBar>,
        deleted: std::collections::BTreeSet<u64>,
    ) {
        if upserts.is_empty() && deleted.is_empty() {
            return;
        }
        if self.force_full {
            return;
        }
        let from = self.sequence;
        let Some(to) = from.checked_add(1) else {
            self.reset_after_change();
            return;
        };
        let bytes = upserts
            .values()
            .map(estimated_update_bar_bytes)
            .sum::<usize>()
            .saturating_add(deleted.len().saturating_mul(24));
        self.sequence = to;
        if upserts.len().saturating_add(deleted.len()) > MAX_UPDATE_ROWS
            || bytes > MAX_UPDATE_JOURNAL_BYTES
        {
            self.reset_at(to);
            return;
        }
        self.batches.push_back(UpdateBatch {
            from,
            to,
            upserts,
            deleted,
            bytes,
        });
        self.bytes = self.bytes.saturating_add(bytes);
        while self.batches.len() > MAX_UPDATE_BATCHES || self.bytes > MAX_UPDATE_JOURNAL_BYTES {
            if let Some(batch) = self.batches.pop_front() {
                self.bytes = self.bytes.saturating_sub(batch.bytes);
            }
        }
    }

    fn replay(
        &self,
        cursor: u64,
    ) -> Option<(BTreeMap<u64, SourceBar>, std::collections::BTreeSet<u64>)> {
        if self.force_full || cursor > self.sequence {
            return None;
        }
        if cursor == self.sequence {
            return Some((BTreeMap::new(), std::collections::BTreeSet::new()));
        }
        let mut expected = cursor;
        let mut upserts = BTreeMap::new();
        let mut deleted = std::collections::BTreeSet::new();
        for batch in self.batches.iter().filter(|batch| batch.to > cursor) {
            if batch.from != expected {
                return None;
            }
            for key in &batch.deleted {
                upserts.remove(key);
                deleted.insert(*key);
            }
            for (key, bar) in &batch.upserts {
                deleted.remove(key);
                upserts.insert(*key, bar.clone());
            }
            expected = batch.to;
        }
        (expected == self.sequence).then_some((upserts, deleted))
    }
}

fn estimated_update_bar_bytes(bar: &SourceBar) -> usize {
    // The fixed reserve covers JSON field names, integer widths, quotes and
    // separators; value lengths account for canonical decimals and epoch text.
    256usize
        .saturating_add(canonical_decimal_text(bar.open).len())
        .saturating_add(canonical_decimal_text(bar.high).len())
        .saturating_add(canonical_decimal_text(bar.low).len())
        .saturating_add(canonical_decimal_text(bar.close).len())
        .saturating_add(canonical_decimal_text(bar.volume).len())
        .saturating_add(canonical_decimal_text(bar.quote_volume).len())
        .saturating_add(bar.source_epoch.len())
}

/// Compact exact source history tuple: decimal OHLCV, availability, source
/// epoch dictionary index, and source sequence. Open times are reconstructed
/// from the contiguous UTC window in Coverage.
pub type WireBar = (
    String,
    String,
    String,
    String,
    String,
    String,
    u64,
    usize,
    u64,
);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub provider_id: String,
    pub venue: String,
    pub market_type: String,
    pub quote_asset: String,
    pub source_kind: String,
}

impl SourceIdentity {
    fn owner() -> Self {
        Self {
            provider_id: "okx-candle-service".into(),
            venue: "okx".into(),
            market_type: "spot".into(),
            quote_asset: "USDC".into(),
            source_kind: "observed".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheBody {
    schema_version: String,
    provider_id: String,
    pair: String,
    saved_at_ms: u64,
    bars_1m: Vec<SourceBar>,
    bars_1h: Vec<SourceBar>,
    bars_1d: Vec<SourceBar>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheRecord {
    body: CacheBody,
    sha256: String,
}

#[derive(Clone, Debug)]
struct PairHistory {
    minute: VecDeque<SourceBar>,
    hour: VecDeque<SourceBar>,
    day: VecDeque<SourceBar>,
    source_epoch: Arc<str>,
    source_sequence: u64,
    projection_sequence: u64,
    available_at_ms: u64,
    last_observed_at_ms: u64,
    ready_source: bool,
    invalid_buckets: BTreeMap<Interval, std::collections::BTreeSet<u64>>,
    update_journals: BTreeMap<Interval, UpdateJournal>,
}
impl PairHistory {
    fn new() -> Self {
        Self {
            minute: VecDeque::with_capacity(MINUTE_ROWS),
            hour: VecDeque::with_capacity(HOUR_ROWS),
            day: VecDeque::with_capacity(DAY_ROWS),
            source_epoch: Arc::from(""),
            source_sequence: 0,
            projection_sequence: 0,
            available_at_ms: 0,
            last_observed_at_ms: 0,
            ready_source: false,
            invalid_buckets: BTreeMap::new(),
            update_journals: [Interval::Minute, Interval::Hour, Interval::Day]
                .into_iter()
                .map(|interval| (interval, UpdateJournal::default()))
                .collect(),
        }
    }
    fn bars(&self, interval: Interval) -> &VecDeque<SourceBar> {
        match interval {
            Interval::Minute => &self.minute,
            Interval::Hour => &self.hour,
            Interval::Day => &self.day,
        }
    }
    fn bars_mut(&mut self, interval: Interval) -> &mut VecDeque<SourceBar> {
        match interval {
            Interval::Minute => &mut self.minute,
            Interval::Hour => &mut self.hour,
            Interval::Day => &mut self.day,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Projection {
    epoch: String,
    started_at_ms: u64,
    pairs: BTreeMap<String, PairHistory>,
}
impl Projection {
    pub fn new(pairs: &[String], started_at_ms: u64) -> Self {
        Self {
            epoch: uuid::Uuid::new_v4().to_string(),
            started_at_ms,
            pairs: pairs
                .iter()
                .map(|pair| (pair.clone(), PairHistory::new()))
                .collect(),
        }
    }

    fn load_record(&mut self, pair: &str, mut body: CacheBody, now_ms: u64) -> Result<()> {
        ensure!(
            body.schema_version == "market-picture.history-cache.v1",
            "cache schema mismatch"
        );
        ensure!(
            body.provider_id == "okx-candle-service" && body.pair == pair,
            "cache identity mismatch"
        );
        ensure!(
            body.saved_at_ms > 0 && body.saved_at_ms <= now_ms,
            "cache saved_at timestamp is invalid"
        );
        let history = self
            .pairs
            .get_mut(pair)
            .context("cache pair outside configured universe")?;
        ensure!(
            body.bars_1m
                .iter()
                .chain(&body.bars_1h)
                .chain(&body.bars_1d)
                .all(|bar| bar.available_at_ms <= body.saved_at_ms),
            "cached source availability follows cache save time"
        );
        let quarantined_minute_rows =
            validate_cache_bars(&mut body.bars_1m, Interval::Minute, MINUTE_ROWS, true)?;
        validate_cache_bars(&mut body.bars_1h, Interval::Hour, HOUR_ROWS, false)?;
        validate_cache_bars(&mut body.bars_1d, Interval::Day, DAY_ROWS, false)?;
        if quarantined_minute_rows > 0 {
            eprintln!(
                "market picture cache quarantined pre-close minute rows pair={pair} count={quarantined_minute_rows}"
            );
        }
        intern_bar_epochs(&mut body.bars_1m);
        intern_bar_epochs(&mut body.bars_1h);
        intern_bar_epochs(&mut body.bars_1d);
        history.minute.extend(body.bars_1m);
        history.hour.extend(body.bars_1h);
        history.day.extend(body.bars_1d);
        for interval in [Interval::Minute, Interval::Hour, Interval::Day] {
            let surface = surface_bounds(history.bars(interval), interval);
            let journal = history
                .update_journals
                .get_mut(&interval)
                .expect("interval journal");
            journal.surface = surface;
            if surface.is_some() {
                journal.sequence = 1;
            }
        }
        // Persisted rows are a baseline only. A current source snapshot must
        // prove freshness after this process starts before any interval is ready.
        history.ready_source = false;
        Ok(())
    }

    pub fn observe(&mut self, event: Event, now_ms: u64) -> Result<()> {
        match event {
            Event::Snapshot { metadata, rows } => {
                let pair = metadata["pair"]
                    .as_str()
                    .context("snapshot pair missing")?
                    .to_owned();
                let epoch: Arc<str> = Arc::from(
                    metadata["epoch"]
                        .as_str()
                        .context("snapshot epoch missing")?,
                );
                let sequence = metadata["sequence"]
                    .as_u64()
                    .context("snapshot sequence missing")?;
                let published = seconds_ms(
                    metadata["published_at"]
                        .as_f64()
                        .context("snapshot publish time missing")?,
                )?;
                let received = seconds_ms(
                    metadata["received_at"]
                        .as_f64()
                        .context("snapshot receive time missing")?,
                )?;
                let bars = rows
                    .iter()
                    .map(|row| {
                        row_to_source(row.as_ref(), &epoch, sequence, received.max(published))
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.observe_rows(SourceObservation {
                    pair,
                    source_epoch: epoch,
                    source_sequence: sequence,
                    published_at_ms: published,
                    received_at_ms: received,
                    bars,
                    snapshot: true,
                    now_ms,
                })
            }
            Event::Other(message) => self.observe_message(message, now_ms),
        }
    }

    fn observe_message(&mut self, message: Value, now_ms: u64) -> Result<()> {
        let kind = message["type"].as_str().unwrap_or_default();
        if kind == "heartbeat" {
            return Ok(());
        }
        let pair = message["pair"]
            .as_str()
            .context("source event pair missing")?
            .to_owned();
        let epoch_text = message["epoch"]
            .as_str()
            .context("source event epoch missing")?;
        let epoch = self
            .pairs
            .get(&pair)
            .filter(|history| history.source_epoch.as_ref() == epoch_text)
            .map(|history| history.source_epoch.clone())
            .unwrap_or_else(|| Arc::from(epoch_text));
        let sequence = message["sequence"]
            .as_u64()
            .context("source event sequence missing")?;
        let published = seconds_ms(
            message["published_at"]
                .as_f64()
                .context("source event publish time missing")?,
        )?;
        if kind == "status" {
            let history = self
                .pairs
                .get_mut(&pair)
                .context("source event pair outside universe")?;
            history.ready_source = false;
            history.source_epoch = epoch;
            history.source_sequence = sequence;
            history.projection_sequence = history.projection_sequence.saturating_add(1);
            return Ok(());
        }
        ensure!(kind == "candle", "unexpected source event type");
        let received = seconds_ms(
            message["received_at"]
                .as_f64()
                .context("source event receive time missing")?,
        )?;
        let candle: Candle =
            serde_json::from_value(message["row"].clone()).context("invalid candle event")?;
        let bar = row_to_source(&candle, &epoch, sequence, received.max(published))?;
        self.observe_rows(SourceObservation {
            pair,
            source_epoch: epoch,
            source_sequence: sequence,
            published_at_ms: published,
            received_at_ms: received,
            bars: vec![bar],
            snapshot: false,
            now_ms,
        })
    }

    fn observe_rows(&mut self, observation: SourceObservation) -> Result<()> {
        let SourceObservation {
            pair,
            source_epoch,
            source_sequence,
            published_at_ms,
            received_at_ms,
            bars,
            snapshot,
            now_ms,
        } = observation;
        ensure!(
            received_at_ms <= now_ms && published_at_ms <= now_ms,
            "source observation is future dated"
        );
        let available_at_ms = received_at_ms.max(published_at_ms);
        let history = self
            .pairs
            .get_mut(&pair)
            .context("source pair outside configured universe")?;
        if !snapshot
            && (history.source_epoch != source_epoch
                || source_sequence != history.source_sequence.saturating_add(1)
                || !history.ready_source)
        {
            history.ready_source = false;
            return Ok(());
        }
        if history.source_epoch != source_epoch && !snapshot {
            return Ok(());
        }
        if history.source_epoch == source_epoch
            && source_sequence < history.source_sequence
            && snapshot
        {
            return Ok(());
        }
        if snapshot {
            validate_source_rows(&bars)?;
        }
        history.source_epoch = source_epoch.clone();
        history.source_sequence = source_sequence;
        history.available_at_ms = history.available_at_ms.max(available_at_ms);
        history.last_observed_at_ms = history.last_observed_at_ms.max(available_at_ms);
        history.ready_source = available_at_ms >= self.started_at_ms
            && now_ms.saturating_sub(available_at_ms) <= 90_000
            && history.last_observed_at_ms >= self.started_at_ms;

        let mut touched = Vec::new();
        let mut changed = false;
        let mut changed_rows = BTreeMap::<Interval, std::collections::BTreeSet<u64>>::new();
        for bar in bars {
            let open_s = bar.open_time_ms / 1000;
            let previous = history
                .minute
                .iter()
                .find(|row| row.open_time_ms == bar.open_time_ms);
            // A source snapshot or correction can revise receipt and epoch
            // lineage without changing OHLCV. Keep the supplied row so the
            // latest row and the response header describe the same revision.
            let row_changed = previous.is_none_or(|existing| existing != &bar);
            changed |= row_changed;
            if row_changed {
                changed_rows
                    .entry(Interval::Minute)
                    .or_default()
                    .insert(bar.open_time_ms);
            }
            let market_changed =
                previous.is_none_or(|existing| !same_market_values(existing, &bar));
            let historical_insert = previous.is_none()
                && history
                    .minute
                    .back()
                    .is_some_and(|last| bar.open_time_ms < last.open_time_ms);
            if market_changed
                && (previous.is_some() || historical_insert || open_s % 3_600 == 3_540)
            {
                touched.push((Interval::Hour, open_s / 3_600 * 3_600));
            }
            if market_changed
                && (previous.is_some() || historical_insert || open_s % 86_400 == 86_340)
            {
                touched.push((Interval::Day, open_s / 86_400 * 86_400));
            }
            insert_sorted(&mut history.minute, bar, MINUTE_ROWS);
        }
        touched.sort_unstable();
        touched.dedup();
        for (interval, start_s) in touched {
            match aggregate_bucket(
                &history.minute,
                interval,
                start_s,
                &source_epoch,
                source_sequence,
                history.available_at_ms,
            )? {
                AggregateOutcome::Complete(bar) => {
                    let invalid = history.invalid_buckets.entry(interval).or_default();
                    invalid.remove(&bar.open_time_ms);
                    let previous = history
                        .bars(interval)
                        .iter()
                        .find(|row| row.open_time_ms == bar.open_time_ms);
                    if previous != Some(&bar) {
                        changed = true;
                        changed_rows
                            .entry(interval)
                            .or_default()
                            .insert(bar.open_time_ms);
                    }
                    insert_sorted(history.bars_mut(interval), bar, interval.expected_rows());
                }
                AggregateOutcome::Incomplete => {
                    let start_ms = start_s.saturating_mul(1_000);
                    let had_row = history
                        .bars_mut(interval)
                        .iter()
                        .any(|row| row.open_time_ms == start_ms);
                    if had_row {
                        history
                            .bars_mut(interval)
                            .retain(|row| row.open_time_ms != start_ms);
                        changed = true;
                        changed_rows.entry(interval).or_default().insert(start_ms);
                    }
                }
                AggregateOutcome::Invalid(reason) => {
                    let start_ms = start_s.saturating_mul(1_000);
                    let newly_invalid = history
                        .invalid_buckets
                        .entry(interval)
                        .or_default()
                        .insert(start_ms);
                    let before = history.bars(interval).len();
                    history
                        .bars_mut(interval)
                        .retain(|row| row.open_time_ms != start_ms);
                    if history.bars(interval).len() != before {
                        changed_rows.entry(interval).or_default().insert(start_ms);
                    }
                    changed = true;
                    if newly_invalid {
                        eprintln!(
                            "market picture {interval:?} aggregate invalid at {start_ms}: {reason:?}"
                        );
                    }
                }
            }
        }
        for interval in [Interval::Minute, Interval::Hour, Interval::Day] {
            let keys = changed_rows.remove(&interval).unwrap_or_default();
            record_update_changes(history, interval, keys);
        }
        if changed || snapshot {
            history.projection_sequence = history.projection_sequence.saturating_add(1);
        }
        Ok(())
    }

    pub fn response(&self, pair: &str, interval: Interval, now_ms: u64) -> Result<HistoryResponse> {
        self.response_parts(pair, interval, now_ms, true)
    }

    pub fn updates(
        &self,
        pair: &str,
        interval: Interval,
        requested_epoch: Option<&str>,
        requested_sequence: Option<u64>,
        now_ms: u64,
    ) -> Result<UpdatesResponse> {
        let history = self
            .pairs
            .get(pair)
            .context("pair outside configured universe")?;
        let journal = history
            .update_journals
            .get(&interval)
            .expect("interval journal");
        let cursor = match (requested_epoch, requested_sequence) {
            (Some(epoch), Some(sequence)) if epoch == self.epoch => {
                journal.replay(sequence).map(|patch| (sequence, patch))
            }
            _ => None,
        };
        if let Some((from_sequence, (upserts, deleted))) = cursor {
            let mut metadata = self.response_parts(pair, interval, now_ms, false)?;
            metadata.schema_version = "market-picture.source-updates.v1".into();
            metadata.sequence = journal.sequence;
            apply_update_availability(&mut metadata, history, interval, now_ms);
            metadata.source_epochs.clear();
            let upserts = upserts.into_values().map(update_bar).collect::<Vec<_>>();
            let updates = UpdatesResponse {
                mode: "delta".into(),
                from_sequence: Some(from_sequence),
                history: metadata,
                upserts,
                deleted_open_time_ms: deleted.into_iter().collect(),
            };
            if serde_json::to_vec(&updates)?.len() > MAX_UPDATE_RESPONSE_BYTES {
                return self.updates(pair, interval, None, None, now_ms);
            }
            return Ok(updates);
        }
        let mut metadata = self.response_parts(pair, interval, now_ms, true)?;
        metadata.schema_version = "market-picture.source-updates.v1".into();
        metadata.sequence = journal.sequence;
        apply_update_availability(&mut metadata, history, interval, now_ms);
        Ok(UpdatesResponse {
            mode: "full".into(),
            from_sequence: None,
            history: metadata,
            upserts: Vec::new(),
            deleted_open_time_ms: Vec::new(),
        })
    }

    fn response_parts(
        &self,
        pair: &str,
        interval: Interval,
        now_ms: u64,
        include_bars: bool,
    ) -> Result<HistoryResponse> {
        let history = self
            .pairs
            .get(pair)
            .context("pair outside configured universe")?;
        let all_bars = history.bars(interval).iter().collect::<Vec<_>>();
        let expected = interval.expected_rows();
        let bounded = if all_bars.len() > expected {
            &all_bars[all_bars.len() - expected..]
        } else {
            &all_bars[..]
        };
        let mut bars = Vec::<&SourceBar>::new();
        for bar in bounded.iter().rev() {
            if bars.last().is_some_and(|newer: &&SourceBar| {
                newer.open_time_ms != bar.open_time_ms + interval.seconds() * 1_000
            }) {
                break;
            }
            bars.push(*bar);
        }
        bars.reverse();
        let gap_count = gaps_refs(bounded, interval.seconds() * 1_000);
        let newest_close = bars.last().map(|bar| bar.close_time_ms);
        let interval_freshness_ms = match interval {
            Interval::Minute => 90_000,
            Interval::Hour => 3_690_000,
            Interval::Day => 86_490_000,
        };
        // Freshness follows the newest completed interval cutoff. A recent minute
        // heartbeat cannot make an old hourly or daily rollup appear current.
        let fresh_cutoff = newest_close.is_some_and(|close| {
            close <= now_ms.saturating_add(90_000)
                && now_ms.saturating_sub(close) <= interval_freshness_ms
        });
        // All intervals depend on the same live minute-source owner. A fresh
        // daily cutoff cannot mask a silent minute stream.
        let fresh_receipt = history.last_observed_at_ms <= now_ms.saturating_add(90_000)
            && now_ms.saturating_sub(history.last_observed_at_ms) <= 90_000;
        let requested_floor = now_ms.saturating_sub(
            (expected as u64).saturating_mul(interval.seconds().saturating_mul(1_000)),
        );
        let invalid_bucket = history
            .invalid_buckets
            .get(&interval)
            .is_some_and(|invalid| invalid.range(requested_floor..=now_ms).next().is_some());
        let fresh_source = history.ready_source
            && history.last_observed_at_ms >= self.started_at_ms
            && fresh_receipt
            && fresh_cutoff;
        let ready = bars.len() == expected && gap_count == 0 && fresh_source && !invalid_bucket;
        let reason = if ready {
            None
        } else if invalid_bucket {
            Some("aggregate_overflow".to_owned())
        } else if history.last_observed_at_ms == 0 && bars.is_empty() {
            Some("warming".to_owned())
        } else if !fresh_source {
            Some("stale".to_owned())
        } else if gap_count > 0 {
            Some("gap".to_owned())
        } else if bars.len() < expected {
            Some("insufficient_history".to_owned())
        } else {
            Some("stale".to_owned())
        };
        let coverage = Coverage {
            expected_count: expected,
            valid_count: bars.len(),
            gap_count,
            first_open_time_ms: bars.first().map(|bar| bar.open_time_ms),
            last_close_time_ms: newest_close,
        };
        // Row availability includes the source publish time, which can follow
        // its receive time. The receive time remains the independent freshness
        // signal above; it cannot serve as this response's availability bound.
        let available_at_ms = bars
            .iter()
            .map(|bar| bar.available_at_ms)
            .max()
            .unwrap_or(0)
            .max(history.available_at_ms);
        // Hourly and daily aggregates retain the revision at which their
        // latest completed bucket was sealed. The minute source can advance
        // many revisions before either aggregate changes again.
        let source_epoch = bars
            .last()
            .map(|bar| bar.source_epoch.to_string())
            .unwrap_or_else(|| {
                if history.source_epoch.is_empty() {
                    "unavailable".to_owned()
                } else {
                    history.source_epoch.to_string()
                }
            });
        let source_sequence = bars
            .last()
            .map(|bar| bar.source_sequence)
            .unwrap_or(history.source_sequence);
        let owned_bars = if include_bars {
            let owned = bars.iter().map(|bar| (*bar).clone()).collect::<Vec<_>>();
            validate_bars(&owned, interval, expected)?;
            owned
        } else {
            Vec::new()
        };
        let cutoff_ms = bars.last().map(|bar| bar.close_time_ms);
        // Even typed stale/unavailable responses need a coherent response
        // envelope. Their `ready`/`reason` fields carry source freshness truth.
        let expires_at_ms = available_at_ms
            .saturating_add(interval_freshness_ms)
            .max(now_ms.saturating_add(1));
        let response = HistoryResponse {
            schema_version: SCHEMA_VERSION.into(),
            encoding: "ohlcv-tuples.v1".into(),
            source: SourceIdentity::owner(),
            pair: pair.to_owned(),
            interval: interval.name().into(),
            epoch: self.epoch.clone(),
            sequence: history.projection_sequence,
            source_epoch,
            source_sequence,
            source_epochs: if include_bars {
                source_epoch_dictionary(&owned_bars)
            } else {
                Vec::new()
            },
            published_at_ms: now_ms,
            available_at_ms,
            expires_at_ms,
            cutoff_ms,
            coverage,
            ready,
            reason,
            bars: if include_bars {
                wire_bars(owned_bars)
            } else {
                Vec::new()
            },
        };
        if include_bars {
            ensure!(
                serde_json::to_vec(&response)?.len() <= MAX_RESPONSE_BYTES,
                "source history response exceeds byte bound"
            );
        }
        Ok(response)
    }

    fn cache_body(&self, pair: &str, saved_at_ms: u64) -> Result<CacheBody> {
        let history = self
            .pairs
            .get(pair)
            .context("pair outside configured universe")?;
        Ok(CacheBody {
            schema_version: "market-picture.history-cache.v1".into(),
            provider_id: "okx-candle-service".into(),
            pair: pair.to_owned(),
            saved_at_ms,
            bars_1m: history.minute.iter().cloned().collect(),
            bars_1h: history.hour.iter().cloned().collect(),
            bars_1d: history.day.iter().cloned().collect(),
        })
    }

    pub fn sequence(&self, pair: &str) -> Option<u64> {
        self.pairs
            .get(pair)
            .map(|history| history.projection_sequence)
    }

    pub fn unavailable(
        pair: &str,
        interval: Interval,
        now_ms: u64,
        reason: &str,
    ) -> HistoryResponse {
        HistoryResponse {
            schema_version: SCHEMA_VERSION.into(),
            source: SourceIdentity::owner(),
            pair: pair.to_owned(),
            encoding: "ohlcv-tuples.v1".into(),
            interval: interval.name().into(),
            epoch: "unavailable".into(),
            sequence: 0,
            source_epoch: "unavailable".into(),
            source_sequence: 0,
            source_epochs: Vec::new(),
            published_at_ms: now_ms,
            available_at_ms: 0,
            expires_at_ms: 0,
            cutoff_ms: None,
            coverage: Coverage {
                expected_count: interval.expected_rows(),
                valid_count: 0,
                gap_count: 0,
                first_open_time_ms: None,
                last_close_time_ms: None,
            },
            ready: false,
            reason: Some(reason.to_owned()),
            bars: Vec::new(),
        }
    }

    pub fn load_cache_bytes(&mut self, pair: &str, bytes: &[u8], now_ms: u64) -> Result<()> {
        ensure!(
            bytes.len() <= MAX_CACHE_BYTES as usize,
            "oversized market picture cache"
        );
        let record: CacheRecord =
            serde_json::from_slice(bytes).context("invalid market picture cache JSON")?;
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&record.body)?));
        ensure!(
            digest == record.sha256,
            "market picture cache checksum mismatch"
        );
        self.load_record(pair, record.body, now_ms)
    }
}

fn seconds_ms(seconds: f64) -> Result<u64> {
    ensure!(
        seconds.is_finite() && seconds >= 0.0 && seconds <= u64::MAX as f64 / 1000.0,
        "invalid source timestamp"
    );
    Ok((seconds * 1000.0).round() as u64)
}

fn row_to_source(
    row: &Candle,
    epoch: &Arc<str>,
    sequence: u64,
    available_at_ms: u64,
) -> Result<SourceBar> {
    ensure!(row.confirmed, "unconfirmed source bar");
    let open_time_ms = row
        .open_time
        .checked_mul(1000)
        .context("source timestamp overflow")?;
    Ok(SourceBar {
        open_time_ms,
        close_time_ms: open_time_ms
            .checked_add(60_000)
            .context("source close timestamp overflow")?,
        open: parse_canonical_decimal(&row.open)?,
        high: parse_canonical_decimal(&row.high)?,
        low: parse_canonical_decimal(&row.low)?,
        close: parse_canonical_decimal(&row.close)?,
        volume: parse_canonical_decimal(&row.volume)?,
        quote_volume: parse_canonical_decimal(&row.quote_volume)?,
        confirmed: true,
        available_at_ms,
        source_epoch: epoch.clone(),
        source_sequence: sequence,
    })
}

fn validate_source_rows(bars: &[SourceBar]) -> Result<()> {
    ensure!(
        bars.len() <= MINUTE_ROWS,
        "source snapshot exceeds minute projection bound"
    );
    for (index, bar) in bars.iter().enumerate() {
        ensure!(
            bar.confirmed
                && bar.close_time_ms == bar.open_time_ms + 60_000
                && bar.open_time_ms % 60_000 == 0,
            "invalid source bar boundary"
        );
        ensure!(
            bar.open_time_ms <= bar.available_at_ms.saturating_add(90_000),
            "source bar is future dated"
        );
        validate_decimal_bar(bar)?;
        if index > 0 {
            ensure!(
                bars[index - 1].close_time_ms == bar.open_time_ms,
                "source snapshot has a gap or unordered rows"
            );
        }
    }
    Ok(())
}

fn intern_bar_epochs(bars: &mut [SourceBar]) {
    let mut epochs = BTreeMap::<String, Arc<str>>::new();
    for bar in bars {
        let shared = epochs
            .entry(bar.source_epoch.to_string())
            .or_insert_with(|| bar.source_epoch.clone())
            .clone();
        bar.source_epoch = shared;
    }
}

fn validate_decimal_bar(bar: &SourceBar) -> Result<()> {
    ensure!(
        bar.open > Decimal::ZERO
            && bar.high > Decimal::ZERO
            && bar.low > Decimal::ZERO
            && bar.close > Decimal::ZERO,
        "nonpositive source price"
    );
    ensure!(
        bar.volume >= Decimal::ZERO && bar.quote_volume >= Decimal::ZERO,
        "negative source volume"
    );
    ensure!(
        bar.high >= bar.open
            && bar.high >= bar.close
            && bar.low <= bar.open
            && bar.low <= bar.close
            && bar.low <= bar.high,
        "invalid source OHLC geometry"
    );
    Ok(())
}

fn validate_bars(bars: &[SourceBar], interval: Interval, max_count: usize) -> Result<()> {
    ensure!(bars.len() <= max_count, "history interval exceeds bound");
    let duration_ms = interval.seconds() * 1000;
    for (index, bar) in bars.iter().enumerate() {
        ensure!(
            bar.confirmed
                && bar.close_time_ms == bar.open_time_ms + duration_ms
                && bar.open_time_ms % duration_ms == 0,
            "invalid interval bar boundary"
        );
        ensure!(
            bar.available_at_ms >= bar.close_time_ms
                && bar.source_sequence > 0
                && !bar.source_epoch.is_empty()
                && bar.source_epoch.len() <= 64
                && bar
                    .source_epoch
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
            "invalid source bar provenance"
        );
        validate_decimal_bar(bar)?;
        if index > 0 {
            ensure!(
                bars[index - 1].close_time_ms == bar.open_time_ms,
                "interval history has a gap or unordered rows"
            );
        }
    }
    Ok(())
}

fn validate_cache_bars(
    bars: &mut Vec<SourceBar>,
    interval: Interval,
    max_count: usize,
    allow_one_preclose: bool,
) -> Result<usize> {
    ensure!(bars.len() <= max_count, "history interval exceeds bound");
    let duration_ms = interval.seconds() * 1000;
    let mut preclose_open = None;
    for (index, bar) in bars.iter().enumerate() {
        ensure!(
            bar.confirmed
                && bar.close_time_ms == bar.open_time_ms + duration_ms
                && bar.open_time_ms % duration_ms == 0,
            "invalid cached interval bar boundary"
        );
        validate_decimal_bar(bar)?;
        ensure!(
            !bar.source_epoch.is_empty()
                && bar.source_epoch.len() <= 64
                && bar
                    .source_epoch
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
                && bar.source_sequence > 0,
            "invalid cached source provenance"
        );
        if bar.available_at_ms < bar.close_time_ms {
            ensure!(
                interval == Interval::Minute
                    && allow_one_preclose
                    && preclose_open.is_none()
                    && bar.close_time_ms - bar.available_at_ms <= MAX_CACHED_PRE_CLOSE_SKEW_MS,
                "invalid cached interval availability"
            );
            preclose_open = Some(bar.open_time_ms);
        }
        if index > 0 {
            ensure!(
                bars[index - 1].open_time_ms < bar.open_time_ms,
                "cached history has unordered or duplicate bars"
            );
        }
    }
    let quarantined = usize::from(preclose_open.is_some());
    if let Some(open_time_ms) = preclose_open {
        bars.retain(|bar| bar.open_time_ms != open_time_ms);
    }
    Ok(quarantined)
}

fn insert_sorted(rows: &mut VecDeque<SourceBar>, bar: SourceBar, capacity: usize) {
    if let Some(position) = rows
        .iter()
        .position(|existing| existing.open_time_ms >= bar.open_time_ms)
    {
        if rows[position].open_time_ms == bar.open_time_ms {
            rows[position] = bar;
        } else {
            rows.insert(position, bar);
        }
    } else {
        rows.push_back(bar);
    }
    while rows.len() > capacity {
        rows.pop_front();
    }
}

fn aggregate_bucket(
    minute: &VecDeque<SourceBar>,
    interval: Interval,
    start_s: u64,
    source_epoch: &Arc<str>,
    source_sequence: u64,
    available_at_ms: u64,
) -> Result<AggregateOutcome> {
    if interval == Interval::Minute {
        return Ok(AggregateOutcome::Incomplete);
    }
    let start_ms = start_s.checked_mul(1000).context("bucket time overflow")?;
    let duration_ms = interval.seconds() * 1000;
    let needed = interval.seconds() / 60;
    let members: Vec<_> = minute
        .iter()
        .filter(|bar| start_ms <= bar.open_time_ms && bar.open_time_ms < start_ms + duration_ms)
        .collect();
    if members.len() != needed as usize
        || members
            .windows(2)
            .any(|pair| pair[1].open_time_ms != pair[0].open_time_ms + 60_000)
    {
        return Ok(AggregateOutcome::Incomplete);
    }
    let sum = |select: fn(&SourceBar) -> Decimal| -> Option<Decimal> {
        members
            .iter()
            .try_fold(Decimal::ZERO, |total, bar| total.checked_add(select(bar)))
    };
    let Some(volume) = sum(|bar| bar.volume) else {
        return Ok(AggregateOutcome::Invalid(
            AggregateInvalidReason::VolumeOverflow,
        ));
    };
    let Some(quote_volume) = sum(|bar| bar.quote_volume) else {
        return Ok(AggregateOutcome::Invalid(
            AggregateInvalidReason::QuoteVolumeOverflow,
        ));
    };
    let high = members
        .iter()
        .map(|bar| bar.high)
        .max()
        .context("empty bucket")?;
    let low = members
        .iter()
        .map(|bar| bar.low)
        .min()
        .context("empty bucket")?;
    let first = members.first().context("empty bucket")?;
    let last = members.last().context("empty bucket")?;
    Ok(AggregateOutcome::Complete(SourceBar {
        open_time_ms: start_ms,
        close_time_ms: start_ms + duration_ms,
        open: first.open,
        high,
        low,
        close: last.close,
        volume,
        quote_volume,
        confirmed: true,
        available_at_ms: members
            .iter()
            .map(|bar| bar.available_at_ms)
            .max()
            .unwrap_or(available_at_ms),
        source_epoch: source_epoch.clone(),
        source_sequence,
    }))
}

fn gaps_refs(bars: &[&SourceBar], step_ms: u64) -> usize {
    bars.windows(2)
        .filter(|pair| pair[1].open_time_ms != pair[0].open_time_ms + step_ms)
        .count()
}

fn surface_bounds(rows: &VecDeque<SourceBar>, interval: Interval) -> Option<(u64, u64)> {
    let step_ms = interval.seconds().saturating_mul(1_000);
    let expected = interval.expected_rows();
    let latest = rows.iter().next_back()?.open_time_ms;
    let mut expected_close = latest.checked_add(step_ms)?;
    let mut first = latest;
    for row in rows.iter().rev().take(expected) {
        if row.close_time_ms != expected_close {
            break;
        }
        first = row.open_time_ms;
        expected_close = row.open_time_ms;
    }
    Some((first, latest)).filter(|(first, last)| last >= first && (last - first) % step_ms == 0)
}

fn record_update_changes(
    history: &mut PairHistory,
    interval: Interval,
    changed_keys: std::collections::BTreeSet<u64>,
) {
    let new_surface = surface_bounds(history.bars(interval), interval);
    let old_surface = history
        .update_journals
        .get(&interval)
        .expect("interval journal");
    let old_surface = old_surface.surface;
    if changed_keys.is_empty() && new_surface == old_surface {
        return;
    }
    let step_ms = interval.seconds().saturating_mul(1_000);
    let bounds_len = |bounds: Option<(u64, u64)>| -> usize {
        bounds.map_or(0, |(first, last)| ((last - first) / step_ms + 1) as usize)
    };
    let overlap_len = match (old_surface, new_surface) {
        (Some((old_first, old_last)), Some((new_first, new_last))) => {
            let first = old_first.max(new_first);
            let last = old_last.min(new_last);
            if first <= last {
                ((last - first) / step_ms + 1) as usize
            } else {
                0
            }
        }
        _ => 0,
    };
    let deleted_count = bounds_len(old_surface).saturating_sub(overlap_len);
    let inserted_count = bounds_len(new_surface).saturating_sub(overlap_len);
    let changed_count = changed_keys
        .iter()
        .filter(|key| new_surface.is_some_and(|(first, last)| first <= **key && **key <= last))
        .count();
    if deleted_count > MAX_UPDATE_ROWS
        || inserted_count.saturating_add(changed_count) > MAX_UPDATE_ROWS
    {
        let journal = history
            .update_journals
            .get_mut(&interval)
            .expect("interval journal");
        journal.surface = new_surface;
        journal.reset_after_change();
        return;
    }
    let mut upserts = BTreeMap::new();
    if let Some((first, last)) = new_surface {
        for row in history
            .bars(interval)
            .iter()
            .filter(|row| first <= row.open_time_ms && row.open_time_ms <= last)
        {
            let newly_visible = old_surface.is_none_or(|(old_first, old_last)| {
                row.open_time_ms < old_first || row.open_time_ms > old_last
            });
            if newly_visible || changed_keys.contains(&row.open_time_ms) {
                upserts.insert(row.open_time_ms, row.clone());
            }
        }
    }
    let mut deleted = std::collections::BTreeSet::new();
    if let Some((first, last)) = old_surface {
        let old_keys_are_visible =
            new_surface.is_some_and(|(new_first, new_last)| first <= new_last && new_first <= last);
        if !old_keys_are_visible {
            let journal = history
                .update_journals
                .get_mut(&interval)
                .expect("interval journal");
            journal.surface = new_surface;
            journal.reset_after_change();
            return;
        }
        if let Some((new_first, new_last)) = new_surface {
            let mut key = first;
            while key <= last {
                if key < new_first || key > new_last {
                    deleted.insert(key);
                }
                let Some(next) = key.checked_add(step_ms) else {
                    break;
                };
                key = next;
            }
        }
    }
    let journal = history
        .update_journals
        .get_mut(&interval)
        .expect("interval journal");
    journal.surface = new_surface;
    journal.append(upserts, deleted);
}

fn update_bar(bar: SourceBar) -> UpdateBar {
    UpdateBar {
        open_time_ms: bar.open_time_ms,
        close_time_ms: bar.close_time_ms,
        open: canonical_decimal_text(bar.open),
        high: canonical_decimal_text(bar.high),
        low: canonical_decimal_text(bar.low),
        close: canonical_decimal_text(bar.close),
        volume: canonical_decimal_text(bar.volume),
        quote_volume: canonical_decimal_text(bar.quote_volume),
        confirmed: bar.confirmed,
        available_at_ms: bar.available_at_ms,
        source_epoch: bar.source_epoch.to_string(),
        source_sequence: bar.source_sequence,
    }
}

fn apply_update_availability(
    response: &mut HistoryResponse,
    history: &PairHistory,
    interval: Interval,
    now_ms: u64,
) {
    let surface = history
        .update_journals
        .get(&interval)
        .and_then(|journal| journal.surface);
    response.available_at_ms = surface
        .map(|(first, last)| {
            history
                .bars(interval)
                .iter()
                .filter(|bar| first <= bar.open_time_ms && bar.open_time_ms <= last)
                .map(|bar| bar.available_at_ms)
                .max()
                .unwrap_or(0)
        })
        .unwrap_or_default();
    let interval_freshness_ms = match interval {
        Interval::Minute => 90_000,
        Interval::Hour => 3_690_000,
        Interval::Day => 86_490_000,
    };
    response.expires_at_ms = if response.available_at_ms == 0
        || response.reason.as_deref() == Some("stale")
        || response.reason.as_deref() == Some("warming")
    {
        now_ms.saturating_add(1)
    } else {
        response
            .available_at_ms
            .saturating_add(interval_freshness_ms)
    };
}

fn same_market_values(left: &SourceBar, right: &SourceBar) -> bool {
    left.open_time_ms == right.open_time_ms
        && left.close_time_ms == right.close_time_ms
        && left.open == right.open
        && left.high == right.high
        && left.low == right.low
        && left.close == right.close
        && left.volume == right.volume
        && left.quote_volume == right.quote_volume
        && left.confirmed == right.confirmed
}

fn source_epoch_dictionary(bars: &[SourceBar]) -> Vec<String> {
    bars.iter()
        .map(|bar| bar.source_epoch.clone())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|epoch| epoch.to_string())
        .collect()
}

fn wire_bars(bars: Vec<SourceBar>) -> Vec<WireBar> {
    let epochs = source_epoch_dictionary(&bars);
    bars.into_iter()
        .map(|bar| {
            (
                canonical_decimal_text(bar.open),
                canonical_decimal_text(bar.high),
                canonical_decimal_text(bar.low),
                canonical_decimal_text(bar.close),
                canonical_decimal_text(bar.volume),
                canonical_decimal_text(bar.quote_volume),
                bar.available_at_ms,
                epochs
                    .binary_search_by(|candidate| candidate.as_str().cmp(bar.source_epoch.as_ref()))
                    .expect("source epoch dictionary is complete"),
                bar.source_sequence,
            )
        })
        .collect()
}

#[derive(Clone)]
pub struct ProjectionCache {
    dir: PathBuf,
}
impl ProjectionCache {
    pub fn open(root: &Path) -> Result<Self> {
        let dir = root.join("market-picture");
        if !dir.exists() {
            fs::DirBuilder::new().recursive(true).create(&dir)?;
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        }
        let metadata = fs::symlink_metadata(&dir)?;
        ensure!(
            metadata.is_dir() && metadata.permissions().mode() & 0o777 == 0o700,
            "market picture cache directory must be non-symlink mode0700"
        );
        Ok(Self { dir })
    }
    fn path(&self, pair: &str) -> Result<PathBuf> {
        ensure!(crate::model::valid_pair(pair), "invalid cache pair");
        Ok(self.dir.join(format!("{pair}.json")))
    }
    pub fn load(&self, projection: &mut Projection, pair: &str) -> Result<bool> {
        let path = self.path(pair)?;
        let file = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file()
                && metadata.len() <= MAX_CACHE_BYTES
                && metadata.permissions().mode() & 0o777 == 0o600,
            "invalid market picture cache file mode or size"
        );
        let mut bytes = Vec::new();
        file.take(MAX_CACHE_BYTES + 1).read_to_end(&mut bytes)?;
        projection.load_cache_bytes(pair, &bytes, (now() * 1000.0).round() as u64)?;
        Ok(true)
    }
    pub fn load_all(&self, projection: &mut Projection, pairs: &[String]) -> Result<()> {
        for pair in pairs {
            if let Err(error) = self.load(projection, pair) {
                eprintln!("market picture cache ignored {pair}: {error:#}");
            }
        }
        Ok(())
    }
    pub fn save(&self, projection: &Projection, pair: &str, saved_at_ms: u64) -> Result<()> {
        self.save_body(projection.cache_body(pair, saved_at_ms)?)
    }

    fn save_body(&self, body: CacheBody) -> Result<()> {
        let pair = body.pair.clone();
        let rows = body.bars_1m.len() + body.bars_1h.len() + body.bars_1d.len();
        ensure!(
            rows <= MINUTE_ROWS + HOUR_ROWS + DAY_ROWS,
            "market picture cache row bound exceeded"
        );
        let record = CacheRecord {
            sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body)?)),
            body,
        };
        let bytes = serde_json::to_vec(&record)?;
        ensure!(
            bytes.len() as u64 <= MAX_CACHE_BYTES,
            "market picture cache exceeds per-pair byte bound"
        );
        let current_size = fs::read_dir(&self.dir)?
            .filter_map(Result::ok)
            .filter_map(|entry| entry.metadata().ok())
            .map(|meta| meta.len())
            .sum::<u64>();
        let target = self.path(&pair)?;
        let old_size = match fs::symlink_metadata(&target) {
            Ok(meta) => {
                ensure!(
                    meta.is_file() && meta.permissions().mode() & 0o777 == 0o600,
                    "invalid existing market picture cache file"
                );
                meta.len()
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        ensure!(
            current_size.saturating_sub(old_size)
                <= MAX_CACHE_TOTAL_BYTES.saturating_sub(bytes.len() as u64),
            "market picture cache total byte bound exceeded"
        );
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
}

pub type SharedProjection = Arc<Mutex<Projection>>;

pub async fn run(
    projection: SharedProjection,
    source: Shared,
    cache: ProjectionCache,
    stop: CancellationToken,
) {
    let mut receiver = source.lock().await.events.subscribe();
    let mut flush = interval(Duration::from_secs(5));
    let mut saved = BTreeMap::<String, u64>::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            event = receiver.recv() => match event {
                Ok(event) => {
                    let now_ms = (now() * 1000.0).round() as u64;
                    if let Err(error) = projection.lock().await.observe(event, now_ms) {
                        eprintln!("market picture source event rejected: {error:#}");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    for history in projection.lock().await.pairs.values_mut() { history.ready_source = false; }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            },
            _ = flush.tick() => flush_projection(&projection, &cache, &mut saved, false).await,
        }
    }
    flush_projection(&projection, &cache, &mut saved, true).await;
}

async fn flush_projection(
    projection: &SharedProjection,
    cache: &ProjectionCache,
    saved: &mut BTreeMap<String, u64>,
    final_flush: bool,
) {
    let pairs = projection
        .lock()
        .await
        .pairs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for pair in pairs {
        let saved_at_ms = (now() * 1000.0).round() as u64;
        let snapshot = {
            let owner = projection.lock().await;
            let sequence = owner.sequence(&pair).unwrap_or_default();
            if sequence == 0 || (!final_flush && saved.get(&pair) == Some(&sequence)) {
                None
            } else {
                match owner.cache_body(&pair, saved_at_ms) {
                    Ok(body) => Some((sequence, body)),
                    Err(error) => {
                        eprintln!("market picture cache snapshot failed {pair}: {error:#}");
                        None
                    }
                }
            }
        };
        let Some((sequence, body)) = snapshot else {
            continue;
        };
        let cache = cache.clone();
        let cache_pair = pair.clone();
        match tokio::task::spawn_blocking(move || cache.save_body(body)).await {
            Ok(Ok(())) => {
                saved.insert(pair, sequence);
            }
            Ok(Err(error)) => {
                eprintln!("market picture cache write failed {cache_pair}: {error:#}")
            }
            Err(error) => eprintln!("market picture cache task failed {cache_pair}: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bar(open_time_ms: u64, close: &str) -> SourceBar {
        SourceBar {
            open_time_ms,
            close_time_ms: open_time_ms + 60_000,
            open: parse_canonical_decimal(close).unwrap(),
            high: parse_canonical_decimal(close).unwrap(),
            low: parse_canonical_decimal(close).unwrap(),
            close: parse_canonical_decimal(close).unwrap(),
            volume: Decimal::ONE,
            quote_volume: parse_canonical_decimal(close).unwrap(),
            confirmed: true,
            available_at_ms: open_time_ms + 60_000,
            source_epoch: "source-epoch".into(),
            source_sequence: 1,
        }
    }

    fn seeded_minute_projection(times: &[u64]) -> Projection {
        let pair = "BTC-USDC".to_owned();
        let mut projection = Projection::new(std::slice::from_ref(&pair), 1);
        let history = projection.pairs.get_mut(&pair).unwrap();
        history.minute = times
            .iter()
            .map(|open| bar(*open, "100"))
            .collect::<VecDeque<_>>();
        let journal = history.update_journals.get_mut(&Interval::Minute).unwrap();
        journal.surface = surface_bounds(&history.minute, Interval::Minute);
        journal.sequence = 1;
        projection
    }

    #[test]
    fn updates_expand_and_shrink_the_contiguous_gap_tail_with_explicit_patches() {
        let pair = "BTC-USDC";
        let mut projection = seeded_minute_projection(&[0, 60_000, 120_000, 240_000, 300_000]);
        let initial = projection
            .updates(pair, Interval::Minute, None, None, 400_000)
            .unwrap();
        assert_eq!(initial.mode, "full");
        assert_eq!(initial.history.coverage.valid_count, 2);
        assert_eq!(initial.history.coverage.gap_count, 1);
        assert_eq!(initial.history.coverage.first_open_time_ms, Some(240_000));
        assert_eq!(initial.history.sequence, 1);

        let history = projection.pairs.get_mut(pair).unwrap();
        insert_sorted(&mut history.minute, bar(180_000, "100"), MINUTE_ROWS);
        record_update_changes(history, Interval::Minute, [180_000].into_iter().collect());
        let expanded = projection
            .updates(
                pair,
                Interval::Minute,
                Some(&projection.epoch),
                Some(1),
                400_000,
            )
            .unwrap();
        assert_eq!(expanded.mode, "delta");
        assert_eq!(expanded.from_sequence, Some(1));
        assert_eq!(expanded.history.sequence, 2);
        assert_eq!(expanded.history.coverage.valid_count, 6);
        assert_eq!(expanded.history.coverage.gap_count, 0);
        let expanded_keys = expanded
            .upserts
            .iter()
            .map(|bar| bar.open_time_ms)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            expanded_keys,
            [0, 60_000, 120_000, 180_000].into_iter().collect()
        );
        assert!(expanded.deleted_open_time_ms.is_empty());

        let history = projection.pairs.get_mut(pair).unwrap();
        history.minute.retain(|bar| bar.open_time_ms != 180_000);
        record_update_changes(history, Interval::Minute, [180_000].into_iter().collect());
        let shrunk = projection
            .updates(
                pair,
                Interval::Minute,
                Some(&projection.epoch),
                Some(2),
                400_000,
            )
            .unwrap();
        assert_eq!(shrunk.mode, "delta");
        assert_eq!(shrunk.history.coverage.valid_count, 2);
        assert_eq!(shrunk.history.coverage.first_open_time_ms, Some(240_000));
        assert_eq!(shrunk.deleted_open_time_ms, [0, 60_000, 120_000, 180_000]);
        assert!(shrunk.upserts.is_empty());
        let coalesced = projection
            .updates(
                pair,
                Interval::Minute,
                Some(&projection.epoch),
                Some(1),
                400_000,
            )
            .unwrap();
        assert_eq!(coalesced.mode, "delta");
        assert_eq!(coalesced.history.sequence, 3);
        assert!(coalesced.upserts.is_empty());
        assert_eq!(
            coalesced.deleted_open_time_ms,
            [0, 60_000, 120_000, 180_000]
        );
    }

    #[test]
    fn update_journal_byte_estimate_covers_serialized_patch_rows() {
        let mut value = bar(60_000, "1234567890123456789012345678");
        value.source_epoch = "e".repeat(64).into();
        value.source_sequence = u64::MAX;
        let serialized_len = serde_json::to_vec(&update_bar(value.clone()))
            .unwrap()
            .len();
        assert!(estimated_update_bar_bytes(&value) >= serialized_len);
    }

    #[test]
    fn updates_force_full_for_restart_unknown_epoch_and_evicted_cursor() {
        let pair = "BTC-USDC";
        let mut projection = seeded_minute_projection(&[0, 60_000, 120_000]);
        let old_epoch = projection.epoch.clone();
        let restarted = Projection::new(&[pair.to_owned()], 2);
        assert_eq!(
            restarted
                .updates(pair, Interval::Minute, Some(&old_epoch), Some(1), 3)
                .unwrap()
                .mode,
            "full"
        );
        assert_eq!(
            projection
                .updates(pair, Interval::Minute, Some("unknown"), Some(1), 3)
                .unwrap()
                .mode,
            "full"
        );
        assert_eq!(
            projection
                .updates(pair, Interval::Minute, Some(&old_epoch), Some(2), 3)
                .unwrap()
                .mode,
            "full"
        );

        for revision in 0..=MAX_UPDATE_BATCHES {
            let history = projection.pairs.get_mut(pair).unwrap();
            let row = history.minute.back_mut().unwrap();
            let changed = Decimal::from(101 + revision as u64);
            row.open = changed;
            row.high = changed;
            row.low = changed;
            row.close = changed;
            record_update_changes(history, Interval::Minute, [120_000].into_iter().collect());
        }
        assert_eq!(
            projection
                .updates(pair, Interval::Minute, Some(&old_epoch), Some(1), 3)
                .unwrap()
                .mode,
            "full"
        );
        let current = projection.pairs[pair].update_journals[&Interval::Minute].sequence;
        assert_eq!(
            projection
                .updates(pair, Interval::Minute, Some(&old_epoch), Some(current), 3)
                .unwrap()
                .mode,
            "delta"
        );

        let mut exhausted = seeded_minute_projection(&[0, 60_000, 120_000]);
        let exhausted_epoch = exhausted.epoch.clone();
        let history = exhausted.pairs.get_mut(pair).unwrap();
        history
            .update_journals
            .get_mut(&Interval::Minute)
            .unwrap()
            .sequence = u64::MAX;
        let latest = history.minute.back_mut().unwrap();
        latest.close = Decimal::from(101);
        latest.high = Decimal::from(101);
        record_update_changes(history, Interval::Minute, [120_000].into_iter().collect());
        assert_eq!(
            exhausted
                .updates(
                    pair,
                    Interval::Minute,
                    Some(&exhausted_epoch),
                    Some(u64::MAX),
                    3,
                )
                .unwrap()
                .mode,
            "full"
        );
    }

    #[test]
    fn update_metadata_is_interval_scoped_but_freshness_remains_current() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = 1_800_000_000_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 60_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        let mut hourly = bar(now_ms / 3_600_000 * 3_600_000 - 3_600_000, "100");
        hourly.close_time_ms = hourly.open_time_ms + 3_600_000;
        hourly.available_at_ms = now_ms;
        history.hour.push_back(hourly);
        history.source_epoch = "source-epoch".into();
        history.source_sequence = 2;
        history.available_at_ms = now_ms;
        history.last_observed_at_ms = now_ms;
        history.ready_source = true;
        let journal = history.update_journals.get_mut(&Interval::Hour).unwrap();
        journal.surface = surface_bounds(&history.hour, Interval::Hour);
        journal.sequence = 1;

        let epoch = projection.epoch.clone();
        let first = projection
            .updates(&pair, Interval::Hour, None, None, now_ms)
            .unwrap();
        let first_available = first.history.available_at_ms;
        let first_expiry = first.history.expires_at_ms;
        assert_eq!(first_available, now_ms);
        assert_eq!(first_expiry, now_ms + 3_690_000);

        let history = projection.pairs.get_mut(&pair).unwrap();
        history.available_at_ms += 60_000;
        history.last_observed_at_ms += 60_000;
        history.source_sequence += 1;
        let refreshed = projection
            .updates(
                &pair,
                Interval::Hour,
                Some(&epoch),
                Some(1),
                now_ms + 60_000,
            )
            .unwrap();
        assert_eq!(refreshed.mode, "delta");
        assert!(refreshed.upserts.is_empty());
        assert_eq!(refreshed.history.available_at_ms, first_available);
        assert_eq!(refreshed.history.expires_at_ms, first_expiry);
        assert_eq!(
            refreshed.history.reason,
            Some("insufficient_history".into())
        );

        let stale = projection
            .updates(
                &pair,
                Interval::Hour,
                Some(&epoch),
                Some(1),
                now_ms + 200_000,
            )
            .unwrap();
        assert_eq!(stale.mode, "delta");
        assert_eq!(stale.history.reason, Some("stale".into()));
        assert_eq!(stale.history.expires_at_ms, now_ms + 200_001);
        assert_eq!(stale.history.sequence, 1);
    }

    #[test]
    #[ignore = "one-off JSONL fixture probe for the Python source-update contract"]
    fn source_updates_jsonl_contract_probe() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let latest_closed = now_ms / 60_000 * 60_000 - 60_000;
        let mut projection =
            seeded_minute_projection(&[latest_closed - 120_000, latest_closed - 60_000]);
        {
            let history = projection.pairs.get_mut(&pair).unwrap();
            history.source_epoch = "source-epoch".into();
            history.source_sequence = 1;
            history.available_at_ms = now_ms;
            history.last_observed_at_ms = now_ms;
            history.ready_source = true;
        }
        let epoch = projection.epoch.clone();
        let full = projection
            .updates(&pair, Interval::Minute, None, None, now_ms)
            .unwrap();
        println!("UPDATE_FIXTURE {}", serde_json::to_string(&full).unwrap());

        let history = projection.pairs.get_mut(&pair).unwrap();
        insert_sorted(&mut history.minute, bar(latest_closed, "100"), MINUTE_ROWS);
        record_update_changes(
            history,
            Interval::Minute,
            [latest_closed].into_iter().collect(),
        );
        let appended = projection
            .updates(
                &pair,
                Interval::Minute,
                Some(&epoch),
                Some(full.history.sequence),
                now_ms,
            )
            .unwrap();
        println!(
            "UPDATE_FIXTURE {}",
            serde_json::to_string(&appended).unwrap()
        );

        let history = projection.pairs.get_mut(&pair).unwrap();
        let latest = history.minute.back_mut().unwrap();
        latest.open = Decimal::from(101);
        latest.high = Decimal::from(101);
        latest.low = Decimal::from(101);
        latest.close = Decimal::from(101);
        record_update_changes(
            history,
            Interval::Minute,
            [latest_closed].into_iter().collect(),
        );
        let corrected = projection
            .updates(
                &pair,
                Interval::Minute,
                Some(&epoch),
                Some(appended.history.sequence),
                now_ms,
            )
            .unwrap();
        println!(
            "UPDATE_FIXTURE {}",
            serde_json::to_string(&corrected).unwrap()
        );
    }

    #[test]
    #[ignore = "one-off 300-pair source delta latency probe"]
    fn synthetic_300_pair_updates_probe() {
        let pairs = (0..MAX_MARKET_PICTURE_PAIRS)
            .map(|index| format!("ASSET{index:03}-USDC"))
            .collect::<Vec<_>>();
        let now_ms = (now() * 1000.0).round() as u64;
        let mut projection = Projection::new(&pairs, now_ms);
        for pair in &pairs {
            let history = projection.pairs.get_mut(pair).unwrap();
            for (interval, rows) in [
                (Interval::Minute, MINUTE_ROWS),
                (Interval::Hour, HOUR_ROWS),
                (Interval::Day, DAY_ROWS),
            ] {
                let step_ms = interval.seconds() * 1_000;
                let last = (now_ms / step_ms) * step_ms - step_ms;
                let first = last - (rows as u64 - 1) * step_ms;
                for index in 0..rows {
                    let mut value = bar(first + index as u64 * step_ms, "100");
                    value.close_time_ms = value.open_time_ms + step_ms;
                    value.available_at_ms = value.close_time_ms;
                    history.bars_mut(interval).push_back(value);
                }
                let surface = surface_bounds(history.bars(interval), interval);
                let journal = history.update_journals.get_mut(&interval).unwrap();
                journal.surface = surface;
                journal.sequence = 1;
            }
        }
        let epoch = projection.epoch.clone();
        let start = std::time::Instant::now();
        let mut response_bytes = 0usize;
        for pair in &pairs {
            for interval in [Interval::Minute, Interval::Hour, Interval::Day] {
                let response = projection
                    .updates(pair, interval, Some(&epoch), Some(1), now_ms)
                    .unwrap();
                assert_eq!(response.mode, "delta");
                response_bytes += serde_json::to_vec(&response).unwrap().len();
            }
        }
        let elapsed = start.elapsed();
        println!(
            "synthetic source updates: pairs={}, requests={}, elapsed_ms={}, mean_us_per_request={}, response_bytes_total={response_bytes}",
            pairs.len(),
            pairs.len() * 3,
            elapsed.as_millis(),
            elapsed.as_micros() / (pairs.len() * 3) as u128
        );
    }
    #[test]
    fn maximum_configured_projection_has_a_bounded_compact_buffer_estimate() {
        let estimate = estimated_extension_memory_bytes(MAX_MARKET_PICTURE_PAIRS).unwrap();
        println!(
            "Market Picture max-pair buffer estimate: pairs={MAX_MARKET_PICTURE_PAIRS}, source_bar_bytes={}, projected_bytes={estimate}",
            std::mem::size_of::<SourceBar>()
        );
        assert!(estimate <= MAX_EXTENSION_MEMORY_BYTES);
        assert!(estimated_extension_memory_bytes(240).unwrap() <= MAX_EXTENSION_MEMORY_BYTES);
        assert!(estimated_extension_memory_bytes(MAX_MARKET_PICTURE_PAIRS + 1).is_none());
        assert!(std::mem::size_of::<SourceBar>() <= 160);
    }

    #[test]
    #[ignore = "one-off synthetic max-capacity RSS evidence; run explicitly when auditing the memory bound"]
    fn synthetic_max_pair_preallocation_memory_probe() {
        let before = resource_rss_bytes();
        let mut histories: Vec<_> = (0..MAX_MARKET_PICTURE_PAIRS)
            .map(|_| PairHistory::new())
            .collect();
        let sample = bar(1_800_000_000_000, "1.25");
        for history in &mut histories {
            for _ in 0..MINUTE_ROWS {
                history.minute.push_back(sample.clone());
            }
            for _ in 0..HOUR_ROWS {
                history.hour.push_back(sample.clone());
            }
            for _ in 0..DAY_ROWS {
                history.day.push_back(sample.clone());
            }
        }
        let slots: usize = histories
            .iter()
            .map(|history| {
                history.minute.capacity() + history.hour.capacity() + history.day.capacity()
            })
            .sum();
        assert_eq!(
            slots,
            MAX_MARKET_PICTURE_PAIRS * (MINUTE_ROWS + HOUR_ROWS + DAY_ROWS)
        );
        let after = resource_rss_bytes();
        println!(
            "synthetic max projection: pairs={}, source_bar_bytes={}, reserved_slots={}, estimated_buffer_bytes={}, extension_estimate_bytes={}, maxrss_before_bytes={before}, maxrss_after_bytes={after}",
            MAX_MARKET_PICTURE_PAIRS,
            std::mem::size_of::<SourceBar>(),
            slots,
            slots * std::mem::size_of::<SourceBar>(),
            estimated_extension_memory_bytes(MAX_MARKET_PICTURE_PAIRS).unwrap()
        );
    }

    #[cfg(target_os = "macos")]
    fn resource_rss_bytes() -> u64 {
        let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
        assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) }, 0);
        usage.ru_maxrss as u64
    }

    #[cfg(not(target_os = "macos"))]
    fn resource_rss_bytes() -> u64 {
        let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
        assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) }, 0);
        (usage.ru_maxrss as u64) * 1024
    }

    #[test]
    fn interval_sizes_and_utc_aggregate_boundaries_are_frozen() {
        assert_eq!((MINUTE_ROWS, HOUR_ROWS, DAY_ROWS), (11_521, 3_001, 401));
        let start = 1_800_000_000u64 / 3_600 * 3_600;
        let rows: VecDeque<_> = (0..60).map(|n| bar((start + n * 60) * 1000, "2")).collect();
        let AggregateOutcome::Complete(hour) = aggregate_bucket(
            &rows,
            Interval::Hour,
            start,
            &Arc::from("source-epoch"),
            9,
            start * 1000,
        )
        .unwrap() else {
            panic!("complete hourly bucket expected")
        };
        assert_eq!(
            (hour.open_time_ms, hour.close_time_ms),
            (start * 1000, (start + 3_600) * 1000)
        );
        assert_eq!(hour.quote_volume, Decimal::from(120));
    }
    #[test]
    fn incomplete_or_gapped_utc_bucket_never_becomes_a_confirmed_rollup() {
        let start = 1_800_000_000u64 / 3_600 * 3_600;
        let mut rows: VecDeque<_> = (0..60)
            .filter(|n| *n != 21)
            .map(|n| bar((start + n * 60) * 1000, "2"))
            .collect();
        assert!(matches!(
            aggregate_bucket(
                &rows,
                Interval::Hour,
                start,
                &Arc::from("e"),
                1,
                start * 1000
            )
            .unwrap(),
            AggregateOutcome::Incomplete
        ));
        rows.pop_back();
        assert!(matches!(
            aggregate_bucket(
                &rows,
                Interval::Hour,
                start,
                &Arc::from("e"),
                1,
                start * 1000
            )
            .unwrap(),
            AggregateOutcome::Incomplete
        ));
    }
    #[test]
    fn cache_load_is_a_baseline_and_requires_fresh_source_observation() {
        let pair = "BTC-USDC".to_owned();
        let mut projection = Projection::new(std::slice::from_ref(&pair), 10_000);
        let mut history = PairHistory::new();
        history.minute = (0..MINUTE_ROWS)
            .map(|n| bar(n as u64 * 60_000, "1"))
            .collect();
        let body = CacheBody {
            schema_version: "market-picture.history-cache.v1".into(),
            provider_id: "okx-candle-service".into(),
            pair: pair.clone(),
            saved_at_ms: 4_000_000_000,
            bars_1m: history.minute.iter().cloned().collect(),
            bars_1h: Vec::new(),
            bars_1d: Vec::new(),
        };
        let bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap())),
            body,
        })
        .unwrap();
        projection
            .load_cache_bytes(&pair, &bytes, 4_000_000_100)
            .unwrap();
        let response = projection
            .response(&pair, Interval::Minute, 4_000_000_100)
            .unwrap();
        assert!(!response.ready);
        assert_eq!(response.reason.as_deref(), Some("stale"));
        assert_eq!(response.bars.len(), MINUTE_ROWS);
        assert_eq!(response.available_at_ms, response.bars.last().unwrap().6);
        assert!(response.available_at_ms <= response.published_at_ms);
    }

    #[test]
    fn a_recent_minute_observation_cannot_refresh_an_old_hourly_rollup() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let latest_close = (now_ms / 3_600_000) * 3_600_000 - 2 * 3_600_000;
        let first_open = latest_close - HOUR_ROWS as u64 * 3_600_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        history.hour = (0..HOUR_ROWS)
            .map(|index| {
                let open = first_open + index as u64 * 3_600_000;
                let mut value = bar(open, "1");
                value.close_time_ms = open + 3_600_000;
                value.available_at_ms = value.close_time_ms;
                value.source_epoch = "epoch".into();
                value.source_sequence = index as u64 + 1;
                value
            })
            .collect();
        history.source_epoch = "epoch".into();
        history.source_sequence = 9;
        history.projection_sequence = 1;
        history.last_observed_at_ms = now_ms;
        history.ready_source = true;

        let response = projection.response(&pair, Interval::Hour, now_ms).unwrap();
        assert!(!response.ready);
        assert_eq!(response.reason.as_deref(), Some("stale"));
        assert_eq!(response.coverage.valid_count, HOUR_ROWS);

        let history = projection.pairs.get_mut(&pair).unwrap();
        history.hour.truncate(100);
        let partial = projection.response(&pair, Interval::Hour, now_ms).unwrap();
        assert_eq!(partial.reason.as_deref(), Some("stale"));
    }

    #[test]
    fn fresh_bucket_cutoff_does_not_hide_a_stale_hourly_or_daily_receipt() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        for (interval, expected, step_ms, receipt_age) in [
            (Interval::Hour, HOUR_ROWS, 3_600_000, 180_000),
            (Interval::Day, DAY_ROWS, 86_400_000, 180_000),
        ] {
            let latest_close = (now_ms / step_ms) * step_ms;
            let first_open = latest_close - (expected as u64 - 1) * step_ms;
            let mut projection =
                Projection::new(std::slice::from_ref(&pair), now_ms - receipt_age - 1);
            let history = projection.pairs.get_mut(&pair).unwrap();
            let rows: VecDeque<_> = (0..expected)
                .map(|index| {
                    let open = first_open + index as u64 * step_ms;
                    let mut value = bar(open, "1");
                    value.close_time_ms = open + step_ms;
                    value.available_at_ms = open + step_ms;
                    value.source_epoch = "epoch".into();
                    value.source_sequence = index as u64 + 1;
                    value
                })
                .collect();
            match interval {
                Interval::Hour => history.hour = rows,
                Interval::Day => history.day = rows,
                Interval::Minute => unreachable!(),
            }
            history.source_epoch = "epoch".into();
            history.source_sequence = expected as u64;
            history.projection_sequence = 1;
            history.last_observed_at_ms = now_ms - receipt_age;
            history.ready_source = true;

            let response = projection.response(&pair, interval, now_ms).unwrap();
            assert!(
                !response.ready,
                "{interval:?} must fail closed on stale receipt"
            );
            assert_eq!(response.reason.as_deref(), Some("stale"));
            assert_eq!(response.coverage.valid_count, expected);
            assert!(response.expires_at_ms > response.published_at_ms);
        }
    }

    #[test]
    fn volume_overflow_invalidates_hour_and_day_buckets_without_panicking() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let max = Decimal::MAX;
        for (interval, step_ms, count) in [
            (Interval::Hour, 60_000, 60usize),
            (Interval::Day, 60_000, 1_440usize),
        ] {
            for overflow_quote_volume in [false, true] {
                let bucket_ms = match interval {
                    Interval::Hour => ((now_ms / 3_600_000) - 1) * 3_600_000,
                    Interval::Day => ((now_ms / 86_400_000) - 1) * 86_400_000,
                    Interval::Minute => unreachable!(),
                };
                let first_open = bucket_ms / step_ms * step_ms;
                let bars: Vec<_> = (0..count)
                    .map(|index| {
                        let mut value = bar(first_open + index as u64 * step_ms, "1");
                        value.available_at_ms = value.close_time_ms;
                        value.source_epoch = "overflow-epoch".into();
                        if overflow_quote_volume {
                            value.quote_volume = max;
                        } else {
                            value.volume = max;
                        }
                        value
                    })
                    .collect();
                let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
                let history = projection.pairs.get_mut(&pair).unwrap();
                let mut previous_rollup = bar(bucket_ms, "1");
                previous_rollup.close_time_ms = bucket_ms + interval.seconds() * 1_000;
                previous_rollup.available_at_ms = previous_rollup.close_time_ms;
                previous_rollup.source_epoch = "overflow-epoch".into();
                match interval {
                    Interval::Hour => history.hour.push_back(previous_rollup),
                    Interval::Day => history.day.push_back(previous_rollup),
                    Interval::Minute => unreachable!(),
                }
                let outcome = projection.observe_rows(SourceObservation {
                    pair: pair.clone(),
                    source_epoch: Arc::from("overflow-epoch"),
                    source_sequence: 1,
                    published_at_ms: now_ms,
                    received_at_ms: now_ms,
                    bars,
                    snapshot: true,
                    now_ms,
                });
                assert!(
                    outcome.is_ok(),
                    "overflow is typed data invalidity, not an event failure"
                );
                let response = projection.response(&pair, interval, now_ms).unwrap();
                assert!(!response.ready);
                assert_eq!(response.reason.as_deref(), Some("aggregate_overflow"));
                assert!(response.expires_at_ms > response.published_at_ms);
                assert!(
                    response.bars.is_empty(),
                    "the invalid cutoff must not retain an old rollup"
                );
            }
        }
    }

    #[test]
    fn empty_projection_is_a_typed_warming_response_without_source_identity_claims() {
        let projection = Projection::new(&["BTC-USDC".to_owned()], 1_000);
        let response = projection
            .response("BTC-USDC", Interval::Minute, 2_000)
            .unwrap();
        assert!(!response.ready);
        assert_eq!(response.reason.as_deref(), Some("warming"));
        assert_eq!(response.coverage.expected_count, MINUTE_ROWS);
        assert_eq!(response.coverage.valid_count, 0);
        assert!(response.bars.is_empty());
        assert_eq!(response.source_epoch, "unavailable");
        assert_eq!(response.source_epochs, Vec::<String>::new());
        assert_eq!(response.sequence, 0);
        assert_eq!(response.source_sequence, 0);
        assert_eq!(response.cutoff_ms, None);
        assert_eq!(response.coverage.first_open_time_ms, None);
        assert_eq!(response.coverage.last_close_time_ms, None);
        assert!(response.expires_at_ms > response.published_at_ms);

        let disabled =
            Projection::unavailable("BTC-USDC", Interval::Day, 2_000, "projection_disabled");
        assert_eq!(disabled.reason.as_deref(), Some("projection_disabled"));
        assert_eq!(disabled.cutoff_ms, None);
        assert_eq!(disabled.coverage.first_open_time_ms, None);
        assert_eq!(disabled.coverage.last_close_time_ms, None);
        assert_eq!(disabled.expires_at_ms, 0);
    }

    #[test]
    fn source_publish_lag_and_unchanged_reconnect_keep_response_lineage_coherent() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let open_time = (now_ms / 60_000 - 1) * 60;
        let candle = Arc::new(crate::model::Candle {
            open_time,
            open: "1".into(),
            high: "1".into(),
            low: "1".into(),
            close: "1".into(),
            volume: "1".into(),
            quote_volume: "1".into(),
            confirmed: true,
        });
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 2_000);
        for (epoch, sequence) in [("epoch-a", 1), ("epoch-b", 1), ("epoch-b", 2)] {
            projection
                .observe(
                    Event::Snapshot {
                        metadata: serde_json::json!({
                            "v": 1,
                            "type": "snapshot",
                            "epoch": epoch,
                            "pair": pair,
                            "sequence": sequence,
                            "published_at": now_ms as f64 / 1000.0,
                            "received_at": (now_ms - 1_000) as f64 / 1000.0,
                        }),
                        rows: vec![candle.clone()].into(),
                    },
                    now_ms,
                )
                .unwrap();
            let response = projection
                .response(&pair, Interval::Minute, now_ms)
                .unwrap();
            let newest = response.bars.last().unwrap();
            assert_eq!(response.available_at_ms, now_ms);
            assert_eq!(newest.6, response.available_at_ms);
            assert_eq!(response.source_epochs[newest.7], epoch);
            assert_eq!(newest.8, sequence);
            assert_eq!(response.source_epoch, epoch);
            assert_eq!(response.source_sequence, sequence);
        }
    }

    #[test]
    fn completed_hour_and_day_keep_their_revision_when_minute_source_advances() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let hour_close = now_ms / 3_600_000 * 3_600_000;
        let day_close = now_ms / 86_400_000 * 86_400_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 2_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        let mut hour = bar(hour_close - 3_600_000, "1");
        hour.close_time_ms = hour_close;
        hour.available_at_ms = hour_close;
        let mut day = bar(day_close - 86_400_000, "1");
        day.close_time_ms = day_close;
        day.available_at_ms = day_close;
        history.hour.push_back(hour);
        history.day.push_back(day);
        history.source_epoch = "newer-minute-epoch".into();
        history.source_sequence = 2;
        history.projection_sequence = 2;
        history.available_at_ms = now_ms;
        history.last_observed_at_ms = now_ms;
        history.ready_source = true;
        for interval in [Interval::Hour, Interval::Day] {
            let response = projection.response(&pair, interval, now_ms).unwrap();
            assert_eq!(response.source_epoch, "source-epoch");
            assert_eq!(response.source_sequence, 1);
            assert_eq!(
                response.source_epochs[response.bars[0].7],
                response.source_epoch
            );
            assert_eq!(response.bars[0].8, response.source_sequence);
        }
    }

    #[test]
    fn small_snapshot_preserves_unsupplied_cached_rollups_until_a_market_correction() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let old_day_start = (now_ms / 86_400_000 - 1) * 86_400_000;
        let old_hour_start = old_day_start + 23 * 3_600_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 2_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        history.minute = (0..1_440)
            .map(|index| {
                let mut value = bar(old_day_start + index * 60_000, "1");
                value.source_epoch = "cached-source".into();
                value.source_sequence = 7;
                value
            })
            .collect();
        let cached_hour = match aggregate_bucket(
            &history.minute,
            Interval::Hour,
            old_hour_start / 1000,
            &Arc::from("cached-source"),
            7,
            old_hour_start + 3_600_000,
        )
        .unwrap()
        {
            AggregateOutcome::Complete(value) => value,
            _ => panic!("cached hour fixture must be complete"),
        };
        let cached_day = match aggregate_bucket(
            &history.minute,
            Interval::Day,
            old_day_start / 1000,
            &Arc::from("cached-source"),
            7,
            old_day_start + 86_400_000,
        )
        .unwrap()
        {
            AggregateOutcome::Complete(value) => value,
            _ => panic!("cached day fixture must be complete"),
        };
        history.hour.push_back(cached_hour.clone());
        history.day.push_back(cached_day.clone());
        history.source_epoch = "cached-source".into();
        history.source_sequence = 7;

        let make_candle = |open_time_ms: u64, high: &str, close: &str| {
            Arc::new(crate::model::Candle {
                open_time: open_time_ms / 1000,
                open: "1".into(),
                high: high.into(),
                low: "1".into(),
                close: close.into(),
                volume: "1".into(),
                quote_volume: "1".into(),
                confirmed: true,
            })
        };
        let newest_open = (now_ms / 60_000 - 1) * 60_000;
        for (sequence, row) in [
            (1, make_candle(newest_open, "1", "1")),
            (2, make_candle(old_hour_start + 10 * 60_000, "2", "2")),
        ] {
            projection
                .observe(
                    Event::Snapshot {
                        metadata: serde_json::json!({
                            "v": 1, "type": "snapshot", "epoch": "new-source", "pair": pair,
                            "sequence": sequence, "published_at": now_ms as f64 / 1000.0,
                            "received_at": (now_ms - 1_000) as f64 / 1000.0,
                        }),
                        rows: vec![row].into(),
                    },
                    now_ms,
                )
                .unwrap();
            let hour = projection.pairs[&pair].hour.front().unwrap();
            let day = projection.pairs[&pair].day.front().unwrap();
            if sequence == 1 {
                assert_eq!(
                    hour, &cached_hour,
                    "unsupplied cache must retain its source revision"
                );
                assert_eq!(day, &cached_day);
            } else {
                assert_eq!(hour.source_epoch.as_ref(), "new-source");
                assert_eq!(hour.source_sequence, 2);
                assert_eq!(hour.high, Decimal::TWO);
                assert_eq!(day.source_epoch.as_ref(), "new-source");
                assert_eq!(day.source_sequence, 2);
                assert_eq!(day.high, Decimal::TWO);
            }
        }
    }

    #[test]
    fn historical_gap_fill_completes_hour_and_day_without_a_new_closing_bar() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let day_start = (now_ms / 86_400_000 - 1) * 86_400_000;
        let missing_open = day_start + 100 * 60_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 2_000);
        projection.pairs.get_mut(&pair).unwrap().minute = (0..1_440)
            .filter(|index| *index != 100)
            .map(|index| bar(day_start + index * 60_000, "1"))
            .collect();
        projection
            .observe(
                Event::Snapshot {
                    metadata: serde_json::json!({
                        "v": 1, "type": "snapshot", "epoch": "gap-repair", "pair": pair,
                        "sequence": 1, "published_at": now_ms as f64 / 1000.0,
                        "received_at": (now_ms - 1_000) as f64 / 1000.0,
                    }),
                    rows: vec![Arc::new(crate::model::Candle {
                        open_time: missing_open / 1000,
                        open: "1".into(),
                        high: "1".into(),
                        low: "1".into(),
                        close: "1".into(),
                        volume: "1".into(),
                        quote_volume: "1".into(),
                        confirmed: true,
                    })]
                    .into(),
                },
                now_ms,
            )
            .unwrap();
        let history = &projection.pairs[&pair];
        assert_eq!(history.hour.len(), 1);
        assert_eq!(history.day.len(), 1);
        assert_eq!(history.hour[0].source_epoch.as_ref(), "gap-repair");
        assert_eq!(history.day[0].source_epoch.as_ref(), "gap-repair");
    }

    #[test]
    fn nonready_responses_retain_only_the_latest_contiguous_partial_tail() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let latest = ((now_ms / 60_000) - 1) * 60_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        history.minute = (0..100)
            .map(|index| {
                let mut value = bar(latest - (159 - index as u64) * 60_000, "1");
                value.available_at_ms = now_ms;
                value
            })
            .chain((0..50).map(|index| {
                let mut value = bar(latest - (49 - index as u64) * 60_000, "1");
                value.available_at_ms = now_ms;
                value
            }))
            .collect();
        history.source_epoch = "epoch".into();
        history.source_sequence = 7;
        history.projection_sequence = 1;
        history.available_at_ms = now_ms;
        history.last_observed_at_ms = now_ms;
        history.ready_source = true;

        let response = projection
            .response(&pair, Interval::Minute, now_ms)
            .unwrap();
        assert!(!response.ready);
        assert_eq!(response.reason.as_deref(), Some("gap"));
        assert_eq!(response.coverage.gap_count, 1);
        assert_eq!(response.bars.len(), 50);
        assert_eq!(response.cutoff_ms, response.coverage.last_close_time_ms);
    }

    #[test]
    fn compact_ready_history_fits_the_wire_limit_and_pins_all_revision_epochs() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let latest = ((now_ms / 60_000) - 1) * 60_000;
        let first = latest - (MINUTE_ROWS as u64 - 1) * 60_000;
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 10_000);
        let history = projection.pairs.get_mut(&pair).unwrap();
        history.minute = (0..MINUTE_ROWS)
            .map(|index| {
                let mut value = bar(first + index as u64 * 60_000, "1");
                value.available_at_ms = now_ms;
                value.source_epoch = if index % 2 == 0 { "epoch-a" } else { "epoch-b" }.into();
                value.source_sequence = index as u64 + 1;
                value
            })
            .collect();
        history.source_epoch = "epoch-b".into();
        history.source_sequence = MINUTE_ROWS as u64;
        history.projection_sequence = 9;
        history.available_at_ms = now_ms;
        history.last_observed_at_ms = now_ms;
        history.ready_source = true;

        let response = projection
            .response(&pair, Interval::Minute, now_ms)
            .unwrap();
        assert!(response.ready);
        assert_eq!(response.bars.len(), MINUTE_ROWS);
        assert_eq!(response.epoch, projection.epoch);
        assert_eq!(response.sequence, 9);
        assert_eq!(response.source_epochs, ["epoch-a", "epoch-b"]);
        assert_eq!(response.bars[0].7, 0);
        assert_eq!(response.bars[1].7, 1);
        assert_eq!(response.bars[0].8, 1);
        let bytes = serde_json::to_vec(&response).unwrap();
        assert!(bytes.len() <= MAX_RESPONSE_BYTES, "{} bytes", bytes.len());
        let decoded: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded["bars"][0].as_array().unwrap().len(), 9);
    }

    #[test]
    fn compact_rows_preserve_v1_cache_decimal_and_epoch_wire_strings() {
        let value = bar(1_800_000_000_000, "1.25");
        let encoded = serde_json::to_value(&value).unwrap();
        assert_eq!(encoded["open"], "1.25");
        assert_eq!(encoded["source_epoch"], "source-epoch");
        let decoded: SourceBar = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn cache_quarantines_only_preclose_minute_row_until_fresh_snapshot_replaces_it() {
        let pair = "BTC-USDC".to_owned();
        let now_ms = (now() * 1000.0).round() as u64;
        let latest_open = now_ms / 60_000 * 60_000 - 60_000;
        let first_open = latest_open - (MINUTE_ROWS as u64 - 1) * 60_000;
        let mut bars_1m = (0..MINUTE_ROWS)
            .map(|index| bar(first_open + index as u64 * 60_000, "100"))
            .collect::<Vec<_>>();
        let quarantined_open = bars_1m[MINUTE_ROWS / 2].open_time_ms;
        let quarantined = &mut bars_1m[MINUTE_ROWS / 2];
        quarantined.available_at_ms = quarantined.close_time_ms - 1;
        let body = CacheBody {
            schema_version: "market-picture.history-cache.v1".into(),
            provider_id: "okx-candle-service".into(),
            pair: pair.clone(),
            saved_at_ms: now_ms,
            bars_1m,
            bars_1h: Vec::new(),
            bars_1d: Vec::new(),
        };
        let bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap())),
            body: body.clone(),
        })
        .unwrap();

        let temp =
            std::env::temp_dir().join(format!("market-picture-cache-{}", uuid::Uuid::new_v4()));
        let cache = ProjectionCache::open(&temp).unwrap();
        cache.save_body(body.clone()).unwrap();
        let path = cache.path(&pair).unwrap();
        let original_cache_bytes = fs::read(&path).unwrap();
        let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);

        assert!(cache.load(&mut projection, &pair).unwrap());
        assert_eq!(fs::read(&path).unwrap(), original_cache_bytes);
        let history = &projection.pairs[&pair];
        assert_eq!(history.minute.len(), MINUTE_ROWS - 1);
        assert!(
            history
                .minute
                .iter()
                .all(|row| row.open_time_ms != quarantined_open)
        );
        assert!(!history.ready_source);
        let partial = projection
            .response(&pair, Interval::Minute, now_ms)
            .unwrap();
        assert!(!partial.ready);
        assert!(partial.coverage.gap_count > 0);

        let cached_record: CacheRecord = serde_json::from_slice(&bytes).unwrap();
        let mut malformed = cached_record.body.clone();
        malformed.bars_1m[MINUTE_ROWS / 2].high = Decimal::ZERO;
        let malformed_bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&malformed).unwrap())
            ),
            body: malformed,
        })
        .unwrap();
        let mut rejected = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
        assert!(
            rejected
                .load_cache_bytes(&pair, &malformed_bytes, now_ms)
                .is_err()
        );

        let mut multiple_preclose = cached_record.body.clone();
        multiple_preclose.bars_1m[1].available_at_ms =
            multiple_preclose.bars_1m[1].close_time_ms - 1;
        let multiple_preclose_bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&multiple_preclose).unwrap())
            ),
            body: multiple_preclose,
        })
        .unwrap();
        let mut rejected = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
        assert!(
            rejected
                .load_cache_bytes(&pair, &multiple_preclose_bytes, now_ms)
                .is_err()
        );

        for unavailable_at_ms in [
            cached_record.body.bars_1m[MINUTE_ROWS / 2]
                .close_time_ms
                .saturating_sub(MAX_CACHED_PRE_CLOSE_SKEW_MS + 1),
            0,
        ] {
            let mut outside_skew = cached_record.body.clone();
            outside_skew.bars_1m[MINUTE_ROWS / 2].available_at_ms = unavailable_at_ms;
            let outside_skew_bytes = serde_json::to_vec(&CacheRecord {
                sha256: format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&outside_skew).unwrap())
                ),
                body: outside_skew,
            })
            .unwrap();
            let mut rejected = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
            assert!(
                rejected
                    .load_cache_bytes(&pair, &outside_skew_bytes, now_ms)
                    .is_err()
            );
        }

        let mut hourly_preclose = cached_record.body.clone();
        let hour_open = now_ms / 3_600_000 * 3_600_000 - 3_600_000;
        let mut hour = bar(hour_open, "100");
        hour.close_time_ms = hour_open + 3_600_000;
        hour.available_at_ms = hour.close_time_ms - 1;
        hourly_preclose.bars_1h.push(hour);
        let hourly_preclose_bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&hourly_preclose).unwrap())
            ),
            body: hourly_preclose,
        })
        .unwrap();
        let mut rejected = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
        assert!(
            rejected
                .load_cache_bytes(&pair, &hourly_preclose_bytes, now_ms)
                .is_err()
        );

        let replacement = Candle {
            open_time: quarantined_open / 1000,
            open: "100".into(),
            high: "101".into(),
            low: "99".into(),
            close: "100.5".into(),
            volume: "1".into(),
            quote_volume: "100.5".into(),
            confirmed: true,
        };
        projection
            .observe(
                Event::Snapshot {
                    metadata: serde_json::json!({
                        "v": 1,
                        "type": "snapshot",
                        "epoch": "fresh-source",
                        "pair": pair,
                        "sequence": 1,
                        "published_at": now_ms as f64 / 1000.0,
                        "received_at": now_ms as f64 / 1000.0,
                    }),
                    rows: vec![Arc::new(replacement)].into(),
                },
                now_ms,
            )
            .unwrap();
        let restored = projection
            .response("BTC-USDC", Interval::Minute, now_ms)
            .unwrap();
        assert!(restored.ready);
        assert_eq!(restored.coverage.valid_count, MINUTE_ROWS);
        assert_eq!(restored.bars.len(), MINUTE_ROWS);
        let index = ((quarantined_open - first_open) / 60_000) as usize;
        assert_eq!(restored.bars[index].6, now_ms);
        assert_eq!(
            restored.source_epochs[restored.bars[index].7 as usize],
            "fresh-source"
        );
        assert_eq!(restored.bars[index].8, 1);

        fs::remove_dir_all(temp).unwrap();
    }

    #[test]
    #[ignore = "read-only qualification of the preserved local public-market cache"]
    fn preserved_market_picture_cache_has_only_expected_preclose_rows() {
        let root = std::env::var_os("RSIBOT_MARKET_PICTURE_CACHE_ROOT")
            .expect("set RSIBOT_MARKET_PICTURE_CACHE_ROOT to an existing cache root");
        let root = PathBuf::from(root);
        assert!(root.join("market-picture").is_dir());
        let cache = ProjectionCache::open(&root).unwrap();
        let pairs = ["BNB-USDC", "BTC-USDC", "ETH-USDC", "SOL-USDC", "XRP-USDC"].map(str::to_owned);
        let now_ms = (now() * 1000.0).round() as u64;
        let mut projection = Projection::new(&pairs, now_ms);
        for (pair, expected_rows) in pairs
            .iter()
            .zip([MINUTE_ROWS - 1; 4].into_iter().chain([MINUTE_ROWS]))
        {
            assert!(
                cache.load(&mut projection, pair).unwrap(),
                "missing cache for {pair}"
            );
            let history = &projection.pairs[pair];
            assert_eq!(
                history.minute.len(),
                expected_rows,
                "minute baseline for {pair}"
            );
            assert!(
                !history.ready_source,
                "cache alone must not grant readiness for {pair}"
            );
            println!(
                "pair={pair} retained_minute_rows={} ready=false",
                history.minute.len()
            );
        }
    }

    #[test]
    fn invalid_cache_digest_and_future_saved_time_are_rejected() {
        let pair = "BTC-USDC".to_owned();
        let mut projection = Projection::new(std::slice::from_ref(&pair), 5_000);
        let body = CacheBody {
            schema_version: "market-picture.history-cache.v1".into(),
            provider_id: "okx-candle-service".into(),
            pair: pair.clone(),
            saved_at_ms: 5_001,
            bars_1m: Vec::new(),
            bars_1h: Vec::new(),
            bars_1d: Vec::new(),
        };
        let bytes = serde_json::to_vec(&CacheRecord {
            sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap())),
            body,
        })
        .unwrap();
        assert!(projection.load_cache_bytes(&pair, &bytes, 5_000).is_err());
        let mut corrupt: Value = serde_json::from_slice(&bytes).unwrap();
        corrupt["body"]["pair"] = "ETH-USDC".into();
        assert!(
            projection
                .load_cache_bytes(&pair, &serde_json::to_vec(&corrupt).unwrap(), 6_000)
                .is_err()
        );
    }
}
