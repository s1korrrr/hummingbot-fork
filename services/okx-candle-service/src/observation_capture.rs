//! Bounded public observation capture. Candle collector endpoints remain unchanged.

use crate::{
    observation_config::ObservationSettings,
    observation_journal::{JournalCursor, JournalWorker},
    observations::{
        DedupDecision, FixedTimeBars, TradeDeduplicator, parse_bbo_payload, parse_trade_payload,
        payload_sha256,
    },
};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::broadcast,
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationHealth {
    pub enabled: bool,
    pub running: bool,
    pub environment: Option<String>,
    pub region: Option<String>,
    pub channels: Vec<String>,
    pub source_epochs: BTreeMap<String, String>,
    pub instrument_ids: Vec<String>,
    pub source_profile: Option<String>,
    pub input_schema: String,
    pub last_error: Option<String>,
    pub journal_cursor: Option<JournalCursor>,
    pub last_update_sequence: u64,
    pub last_available_at_ms: Option<u64>,
    pub streams: BTreeMap<String, ObservationStreamHealth>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationStreamHealth {
    pub instrument_id: String,
    pub channel: String,
    pub received_frames: u64,
    pub accepted_observations: u64,
    pub processed_trades: u64,
    pub durable_records: u64,
    pub checkpoints: u64,
    pub duplicates: u64,
    pub conflicts: u64,
    pub gaps: u64,
    pub last_available_at_ms: Option<u64>,
    pub last_quote_age_ms: Option<u64>,
    pub metadata_available_at_ms: Option<u64>,
    pub metadata_observed_revision_sha256: Option<String>,
    pub metadata_qualified: bool,
    pub last_durable_cursor: Option<JournalCursor>,
    /// ACK state is transport evidence only and does not qualify a source.
    pub subscription_acknowledged: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationUpdate {
    pub schema_version: String,
    pub sequence: u64,
    pub epoch: String,
    pub record_kind: String,
    pub channel: Option<String>,
    pub instrument_id: Option<String>,
    pub available_at_ms: Option<u64>,
    /// Local clock at which the record was accepted for journal submission.
    pub accepted_at_ms: Option<u64>,
    /// Local clock after the journal worker acknowledged its durable boundary.
    pub durable_acknowledged_at_ms: Option<u64>,
    /// Local clock when derived computation completed, when applicable.
    pub computed_at_ms: Option<u64>,
    /// Local clock when this update was published to the bounded stream.
    pub published_at_ms: Option<u64>,
    pub quality: String,
    pub reason_codes: Vec<String>,
    pub payload: Value,
    /// `None` means queued or not yet acknowledged by the durable journal.
    pub durable_cursor: Option<JournalCursor>,
}

struct CaptureStateInner {
    health: RwLock<ObservationHealth>,
    next_update_sequence: std::sync::atomic::AtomicU64,
    updates: broadcast::Sender<ObservationUpdate>,
    configured_pairs: BTreeSet<String>,
    max_frame_bytes: usize,
    failed: CancellationToken,
}

/// Shared read model for authenticated observation health and bounded live updates.
/// A receiver that reports `Lagged` must take a new snapshot before resuming.
#[derive(Clone)]
pub struct CaptureState(Arc<CaptureStateInner>);

impl CaptureState {
    /// Bounded status object used when opt-in configuration fails to load. It cannot start capture.
    pub fn unavailable(reason: impl Into<String>) -> Arc<Self> {
        let (updates, _) = broadcast::channel(1);
        Arc::new(Self(Arc::new(CaptureStateInner {
            health: RwLock::new(ObservationHealth {
                enabled: false,
                running: false,
                input_schema: "okx.observations.record.v1".into(),
                last_error: Some(reason.into()),
                ..ObservationHealth::default()
            }),
            next_update_sequence: std::sync::atomic::AtomicU64::new(1),
            updates,
            configured_pairs: BTreeSet::new(),
            max_frame_bytes: 0,
            failed: CancellationToken::new(),
        })))
    }

    pub fn new(settings: &ObservationSettings) -> Result<Arc<Self>> {
        let (updates, _) = broadcast::channel(settings.limits.queue_capacity);
        let pairs = settings
            .instruments
            .iter()
            .map(|item| item.instrument_id.clone())
            .collect::<BTreeSet<_>>();
        let mut health_channels = settings.channels.clone();
        health_channels.push("metadata".into());
        let streams = health_channels
            .iter()
            .flat_map(|channel| {
                pairs.iter().map(move |pair| {
                    (
                        stream_key(channel, pair),
                        ObservationStreamHealth {
                            instrument_id: pair.clone(),
                            channel: channel.clone(),
                            ..ObservationStreamHealth::default()
                        },
                    )
                })
            })
            .collect();
        let initial = ObservationHealth {
            enabled: true,
            environment: Some(settings.environment.clone()),
            region: Some(settings.region.as_str().to_string()),
            channels: settings.channels.clone(),
            source_epochs: BTreeMap::new(),
            instrument_ids: pairs.iter().cloned().collect(),
            streams,
            source_profile: Some(format!("okx_{}_public_v1", settings.region.as_str())),
            input_schema: "okx.observations.record.v1".into(),
            ..ObservationHealth::default()
        };
        Ok(Arc::new(Self(Arc::new(CaptureStateInner {
            health: RwLock::new(initial),
            next_update_sequence: std::sync::atomic::AtomicU64::new(1),
            updates,
            configured_pairs: pairs,
            max_frame_bytes: settings.limits.max_frame_bytes,
            failed: CancellationToken::new(),
        }))))
    }

    pub fn snapshot(&self) -> Result<Value> {
        Ok(serde_json::to_value(self.health_snapshot()?)?)
    }

    pub fn health_snapshot(&self) -> Result<ObservationHealth> {
        self.0
            .health
            .read()
            .map(|state| state.clone())
            .map_err(|_| anyhow::anyhow!("observation health lock poisoned"))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ObservationUpdate> {
        self.0.updates.subscribe()
    }

    pub fn configured_pairs(&self) -> &BTreeSet<String> {
        &self.0.configured_pairs
    }
    pub fn max_frame_bytes(&self) -> usize {
        self.0.max_frame_bytes
    }

    fn failed(&self) -> CancellationToken {
        self.0.failed.clone()
    }

    fn fail(&self, message: String) {
        if let Ok(mut health) = self.0.health.write() {
            health.last_error = Some(message);
        }
        self.0.failed.cancel();
    }

    pub fn set_health(&self, health: ObservationHealth) -> Result<()> {
        *self
            .0
            .health
            .write()
            .map_err(|_| anyhow::anyhow!("observation health lock poisoned"))? = health;
        Ok(())
    }

    pub fn publish(&self, mut update: ObservationUpdate) -> Result<()> {
        update.schema_version = "okx.observations.update.v1".into();
        let mut health = self
            .0
            .health
            .write()
            .map_err(|_| anyhow::anyhow!("observation health lock poisoned"))?;
        update.sequence = self
            .0
            .next_update_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let durable_cursor = update.durable_cursor.clone();
        let available_at_ms = update.available_at_ms;
        if let Some(cursor) = durable_cursor.as_ref()
            && health.journal_cursor.as_ref().is_none_or(|previous| {
                (cursor.segment, cursor.sequence) > (previous.segment, previous.sequence)
            })
        {
            health.journal_cursor = Some(cursor.clone());
        }
        health.last_available_at_ms = health.last_available_at_ms.max(available_at_ms);
        health.last_update_sequence = update.sequence;
        if durable_cursor.is_some()
            && update.payload.get("event").and_then(Value::as_str)
                == Some("exchange_metadata_observed")
            && let Some(instrument_id) = update.instrument_id.as_deref()
        {
            let revision_hash = update
                .payload
                .get("observed_revision_sha256")
                .and_then(Value::as_str)
                .map(str::to_owned);
            for stream in health
                .streams
                .values_mut()
                .filter(|stream| stream.instrument_id == instrument_id)
            {
                stream.metadata_available_at_ms = available_at_ms;
                stream.metadata_observed_revision_sha256 = revision_hash.clone();
                stream.metadata_qualified = true;
            }
        }
        if durable_cursor.is_some()
            && (update.payload.get("event").and_then(Value::as_str) == Some("metadata_rejection")
                || (update.payload.get("event").and_then(Value::as_str) == Some("gap")
                    && update.payload.get("reason").and_then(Value::as_str)
                        == Some("METADATA_UNAVAILABLE")))
            && let Some(instrument_id) = update.instrument_id.as_deref()
        {
            for stream in health
                .streams
                .values_mut()
                .filter(|stream| stream.instrument_id == instrument_id)
            {
                stream.metadata_qualified = false;
            }
        }
        if let Some(instrument_id) = update.instrument_id.as_deref()
            && let Some(channel) = update.channel.as_deref()
            && let Some(stream) = health.streams.get_mut(&stream_key(channel, instrument_id))
        {
            if let Some(cursor) = update.durable_cursor.clone() {
                stream.durable_records = stream.durable_records.saturating_add(1);
                if stream.last_durable_cursor.as_ref().is_none_or(|previous| {
                    (cursor.segment, cursor.sequence) > (previous.segment, previous.sequence)
                }) {
                    stream.last_durable_cursor = Some(cursor);
                }
            }
            match update.record_kind.as_str() {
                "raw_frame" => stream.received_frames = stream.received_frames.saturating_add(1),
                "trade" | "bbo" => {
                    stream.accepted_observations = stream.accepted_observations.saturating_add(1)
                }
                "duplicate" | "bbo_repeat" => {
                    stream.duplicates = stream.duplicates.saturating_add(1)
                }
                "trade_conflict" => stream.conflicts = stream.conflicts.saturating_add(1),
                "gap" => stream.gaps = stream.gaps.saturating_add(1),
                "checkpoint" => stream.checkpoints = stream.checkpoints.saturating_add(1),
                _ => {}
            }
            if update.record_kind == "trade" {
                stream.processed_trades = stream.processed_trades.saturating_add(1);
            }
            if update.record_kind == "subscription_ack" {
                stream.subscription_acknowledged = true;
            }
            stream.last_available_at_ms = stream.last_available_at_ms.max(available_at_ms);
            let observation = update.payload.get("observation").unwrap_or(&update.payload);
            if let (Some(received), Some(event)) = (
                available_at_ms,
                observation.get("event_at_ms").and_then(Value::as_u64),
            ) {
                stream.last_quote_age_ms = received.checked_sub(event);
            }
        }
        // Keep ordering and the health high-water mark atomic with broadcast order.
        let _ = self.0.updates.send(update);
        Ok(())
    }
}

fn stream_key(channel: &str, instrument_id: &str) -> String {
    format!("{channel}|{instrument_id}")
}

fn derived_bar_identity(payload: &Value) -> Result<String> {
    let kind = payload
        .get("record_type")
        .and_then(Value::as_str)
        .context("derived bar kind missing")?;
    let channel = payload
        .get("channel")
        .and_then(Value::as_str)
        .context("derived bar channel missing")?;
    let instrument = payload
        .get("instrument_id")
        .and_then(Value::as_str)
        .context("derived bar instrument missing")?;
    let bar = payload.get("bar").context("derived bar body missing")?;
    let identity = json!({"record_type":kind,"channel":channel,"instrument_id":instrument,
        "start_ms":bar.get("start_ms"),"end_ms":bar.get("end_ms"),
        "trade_ids":bar.get("trade_ids")});
    Ok(payload_sha256(&serde_json::to_vec(&identity)?))
}

fn derived_bar_content_hash(payload: &Value) -> Result<String> {
    let bar = payload.get("bar").context("derived bar body missing")?;
    Ok(payload_sha256(&serde_json::to_vec(bar)?))
}

#[cfg(test)]
fn validate_raw_frame_dispositions(
    entries: &[crate::observation_journal::JournalEntry],
) -> Result<()> {
    let mut pending = BTreeMap::new();
    for entry in entries {
        account_raw_frame_disposition(&mut pending, &entry.payload, usize::MAX)?;
    }
    ensure!(
        pending.is_empty(),
        "journal contains raw frames without a durable normalized or rejection disposition; operator recovery is required"
    );
    Ok(())
}

fn is_terminal_raw_disposition(payload: &Value) -> bool {
    matches!(
        payload.get("record_type").and_then(Value::as_str),
        Some("trade" | "bbo")
    ) || matches!(
        payload.get("event").and_then(Value::as_str),
        Some(
            "duplicate" | "bbo_repeat" | "trade_conflict" | "parse_rejection" | "subscription_ack"
        )
    )
}

type RawFrameKey = (String, String, u64);

fn expected_raw_frame_dispositions(payload: &Value) -> usize {
    payload
        .get("raw_text")
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .and_then(|frame| {
            // An event or ACK is handled as one complete source frame even if
            // it carries a `data` field. Only data frames have per-item exits.
            if frame.get("event").is_some() {
                Some(1)
            } else {
                frame.get("data").and_then(Value::as_array).map(Vec::len)
            }
        })
        .filter(|count| (1..=1000).contains(count))
        .unwrap_or(1)
}

fn account_raw_frame_disposition(
    pending: &mut BTreeMap<RawFrameKey, usize>,
    payload: &Value,
    max_pending: usize,
) -> Result<()> {
    let Some(channel) = payload.get("channel").and_then(Value::as_str) else {
        return Ok(());
    };
    let epoch = payload
        .get("source_epoch")
        .and_then(Value::as_str)
        .unwrap_or("");
    let event = payload.get("event").and_then(Value::as_str).unwrap_or("");
    if event == "raw_frame" {
        let sequence = payload
            .get("receive_sequence")
            .and_then(Value::as_u64)
            .context("raw frame has no receive sequence")?;
        ensure!(
            pending.len() < max_pending,
            "unresolved raw-frame dispositions exceed configured bound"
        );
        ensure!(
            pending
                .insert(
                    (channel.to_owned(), epoch.to_owned(), sequence),
                    expected_raw_frame_dispositions(payload),
                )
                .is_none(),
            "duplicate raw frame sequence in one source epoch"
        );
        return Ok(());
    }
    if !is_terminal_raw_disposition(payload) {
        return Ok(());
    }
    let sequence = payload
        .get("raw_frame_sequence")
        .and_then(Value::as_u64)
        .or_else(|| {
            (event == "subscription_ack")
                .then(|| payload.get("receive_sequence").and_then(Value::as_u64))
                .flatten()
        });
    if let Some(sequence) = sequence {
        let key = (channel.to_owned(), epoch.to_owned(), sequence);
        let reason = payload.get("reason").and_then(Value::as_str).unwrap_or("");
        if matches!(
            reason,
            "INVALID_BATCH_SIZE"
                | "DATA_NOT_ARRAY"
                | "CHANNEL_MISSING"
                | "INSTRUMENT_MISSING"
                | "INSTRUMENT_OUTSIDE_CATALOGUE"
                | "UNREQUESTED_CHANNEL"
        ) {
            pending.remove(&key);
        } else if let Some(remaining) = pending.get_mut(&key) {
            *remaining -= 1;
            if *remaining == 0 {
                pending.remove(&key);
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct ChannelReplayInventory {
    earliest_trade_event_ms: Option<u64>,
    has_history: bool,
    suffix_records: usize,
    output_hashes: BTreeMap<String, String>,
    checkpoint_seen: bool,
    after_checkpoint: bool,
}

fn validate_raw_frame_dispositions_stream(
    root: &std::path::Path,
    max_pending: usize,
) -> Result<()> {
    let mut pending = BTreeMap::new();
    crate::observation_journal::visit_verified_records(root, |entry| {
        account_raw_frame_disposition(&mut pending, &entry.payload, max_pending)
    })?;
    ensure!(
        pending.is_empty(),
        "journal contains raw frames without a durable normalized or rejection disposition; operator recovery is required"
    );
    Ok(())
}

fn inventory_replay_suffixes(
    root: &std::path::Path,
    checkpoints: &BTreeMap<String, Option<crate::observations::ReplayCheckpoint>>,
    max_records: usize,
) -> Result<BTreeMap<String, ChannelReplayInventory>> {
    let mut inventories = checkpoints
        .iter()
        .map(|(channel, checkpoint)| {
            let inventory = ChannelReplayInventory {
                after_checkpoint: checkpoint.is_none(),
                ..ChannelReplayInventory::default()
            };
            (channel.clone(), inventory)
        })
        .collect::<BTreeMap<_, _>>();
    crate::observation_journal::visit_verified_records(root, |entry| {
        let Some(channel) = entry.payload.get("channel").and_then(Value::as_str) else {
            return Ok(());
        };
        let Some(inventory) = inventories.get_mut(channel) else {
            return Ok(());
        };
        inventory.has_history = true;
        if entry.payload.get("record_type").and_then(Value::as_str) == Some("trade")
            && let Some(event_at_ms) = entry
                .payload
                .get("observation")
                .and_then(|v| v.get("event_at_ms"))
                .and_then(Value::as_u64)
        {
            inventory.earliest_trade_event_ms = Some(
                inventory
                    .earliest_trade_event_ms
                    .map_or(event_at_ms, |value| value.min(event_at_ms)),
            );
        }
        if !inventory.after_checkpoint {
            if checkpoints[channel]
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.cursor == entry.cursor)
            {
                inventory.checkpoint_seen = true;
                inventory.after_checkpoint = true;
            }
            return Ok(());
        }
        inventory.suffix_records = inventory.suffix_records.saturating_add(1);
        ensure!(
            inventory.suffix_records <= max_records,
            "verified observation replay suffix exceeds max_replay_records"
        );
        if matches!(
            entry.payload.get("record_type").and_then(Value::as_str),
            Some("time_bar" | "notional_bar")
        ) {
            let identity = derived_bar_identity(&entry.payload)?;
            let content_hash = derived_bar_content_hash(&entry.payload)?;
            if let Some(previous) = inventory
                .output_hashes
                .insert(identity, content_hash.clone())
            {
                ensure!(
                    previous == content_hash,
                    "durable derived bar identity has conflicting content"
                );
            }
        }
        Ok(())
    })?;
    for (channel, checkpoint) in checkpoints {
        if checkpoint.is_some() {
            ensure!(
                inventories[channel].checkpoint_seen,
                "replay checkpoint cursor is absent from the verified journal"
            );
        }
    }
    Ok(inventories)
}

fn exact_bytes_field(raw: &[u8]) -> Value {
    match std::str::from_utf8(raw) {
        Ok(text) => json!({"raw_text":text}),
        Err(_) => {
            json!({"raw_bytes_hex":raw.iter().map(|byte| format!("{byte:02x}")).collect::<String>()})
        }
    }
}

async fn metadata_gap_and_rejection(
    settings: &ObservationSettings,
    journal: &JournalWorker,
    state: &Arc<CaptureState>,
    available_at_ms: u64,
    reason: &str,
) -> Result<()> {
    for channel in &settings.channels {
        for instrument in &settings.instruments {
            let rejection = json!({"schema_version":"okx.observations.rejection.v1",
                "event":"metadata_rejection","channel":channel,"instrument_id":instrument.instrument_id,
                "metadata_revision":instrument.metadata_revision,"reason":reason,
                "available_at_ms":available_at_ms});
            submit_record(journal, state, rejection).await?;
            let gap = json!({"schema_version":"okx.observations.source-event.v1","event":"gap",
                "channel":channel,"instrument_id":instrument.instrument_id,"reason":"METADATA_UNAVAILABLE",
                "available_at_ms":available_at_ms,"qualified_source":false});
            submit_record(journal, state, gap).await?;
        }
    }
    Ok(())
}

async fn observe_metadata_once(
    settings: &ObservationSettings,
    journal: &JournalWorker,
    state: &Arc<CaptureState>,
) -> Result<()> {
    let (raw, available_at_ms, http_status) = match crate::observation_metadata::fetch_metadata(
        settings.region,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            let rejection = json!({"schema_version":"okx.observations.rejection.v1",
                "event":"metadata_fetch_rejection","channel":"metadata","region":settings.region.as_str(),
                "reason":"FETCH_FAILED","available_at_ms":wall_ms()?});
            submit_record(journal, state, rejection).await?;
            metadata_gap_and_rejection(
                settings,
                journal,
                state,
                wall_ms()?,
                "METADATA_FETCH_FAILED",
            )
            .await?;
            return Err(error).context("public instrument metadata refresh failed");
        }
    };
    ensure!(
        raw.len() <= 1024 * 1024,
        "metadata response exceeds shared 1 MiB source bound"
    );
    let raw_sha256 = payload_sha256(&raw);
    let mut response = json!({"schema_version":"okx.observations.raw-metadata.v1","event":"metadata_response",
        "channel":"metadata","venue":"okx","environment":settings.environment,
        "region":settings.region.as_str(),"http_status":http_status,"available_at_ms":available_at_ms,"raw_sha256":raw_sha256});
    if let Value::Object(fields) = exact_bytes_field(&raw) {
        fields.into_iter().for_each(|(key, value)| {
            response[key] = value;
        });
    }
    submit_record(journal, state, response).await?;
    if !(200..300).contains(&http_status) {
        metadata_gap_and_rejection(
            settings,
            journal,
            state,
            available_at_ms,
            "METADATA_HTTP_STATUS",
        )
        .await?;
        bail!("metadata HTTP endpoint returned status {http_status}");
    }
    let receipt = match crate::observation_metadata::decode_metadata(
        &raw,
        &settings.instruments,
        available_at_ms,
    ) {
        Ok(receipt) => receipt,
        Err(error) => {
            metadata_gap_and_rejection(
                settings,
                journal,
                state,
                available_at_ms,
                "METADATA_CATALOG_INVALID",
            )
            .await?;
            return Err(error).context("public instrument metadata differs from frozen catalog");
        }
    };
    for observation in receipt.observations {
        submit_record(journal, state, observation).await?;
    }
    Ok(())
}

async fn refresh_metadata(
    settings: ObservationSettings,
    journal: JournalWorker,
    state: Arc<CaptureState>,
    stop: CancellationToken,
) -> Result<()> {
    let period = Duration::from_millis(settings.limits.metadata_refresh_interval_ms);
    let mut refresh = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = refresh.tick() => observe_metadata_once(&settings, &journal, &state).await?,
        }
    }
}

pub async fn run(settings: ObservationSettings, stop: CancellationToken) -> Result<()> {
    let state = CaptureState::new(&settings)?;
    run_with_state(settings, state, stop).await
}

pub fn websocket_endpoint(settings: &ObservationSettings, channel: &str) -> Result<&'static str> {
    match channel {
        "trades-all" => Ok(settings.region.business_ws_url()),
        "bbo-tbt" => Ok(settings.region.public_ws_url()),
        _ => bail!("unsupported observation channel"),
    }
}

async fn drain_capture_tasks(
    tasks: &mut JoinSet<Result<()>>,
    first: &mut Result<()>,
    max_wait: Duration,
) -> Result<()> {
    if timeout(max_wait, async {
        while let Some(result) = tasks.join_next().await {
            if first.is_ok() {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => *first = Err(error),
                    Err(error) => {
                        *first = Err(anyhow::anyhow!(
                            "observation channel task panicked: {error}"
                        ))
                    }
                }
            }
        }
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        bail!(
            "observation shutdown drain exceeded {} seconds; operator must verify raw-frame dispositions before restart",
            max_wait.as_secs()
        );
    }
    Ok(())
}

pub async fn run_with_state(
    settings: ObservationSettings,
    state: Arc<CaptureState>,
    stop: CancellationToken,
) -> Result<()> {
    let journal = JournalWorker::start_with_disk_floor(
        &settings.journal_dir,
        settings.limits.segment_bytes,
        settings.limits.queue_capacity,
        settings.limits.minimum_disk_free_bytes,
    )?;
    let mut health = state.health_snapshot()?;
    health.running = true;
    health.last_error = None;
    state.set_health(health)?;
    let instruments = settings
        .instruments
        .iter()
        .map(|item| item.instrument_id.clone())
        .collect::<BTreeSet<_>>();
    let configuration_identity = settings.observation_identity()?;
    let thresholds = settings
        .notional_bars
        .iter()
        .map(|item| {
            let threshold = rust_decimal::Decimal::from_str_exact(&item.quote_threshold)
                .context("invalid notional bar threshold")?;
            Ok((item.instrument_id.clone(), threshold))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    validate_raw_frame_dispositions_stream(
        std::path::Path::new(&settings.journal_dir),
        settings
            .limits
            .queue_capacity
            .saturating_mul(1000)
            .max(1000),
    )?;
    let checkpoints = settings
        .channels
        .iter()
        .map(|channel| {
            Ok((
                channel.clone(),
                crate::observations::ObservationReplay::latest_checkpoint(
                    &settings.journal_dir,
                    channel,
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let inventory = inventory_replay_suffixes(
        std::path::Path::new(&settings.journal_dir),
        &checkpoints,
        settings.limits.max_replay_records,
    )?;
    let capture_start_ms = wall_ms()?;
    let mut replays = BTreeMap::new();
    for channel in &settings.channels {
        let checkpoint = checkpoints[channel].clone();
        let (mut replay, after) = if let Some(checkpoint) = checkpoint {
            ensure!(
                checkpoint.dedup.limit == settings.limits.dedup_identities,
                "configured dedup limit differs from durable checkpoint"
            );
            ensure!(
                checkpoint
                    .time_bars
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    == instruments,
                "configured instruments differ from durable observation checkpoint"
            );
            ensure!(
                checkpoint
                    .notional_bars
                    .keys()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    == thresholds.keys().cloned().collect(),
                "configured notional bar calibrations differ from durable observation checkpoint"
            );
            let cursor = checkpoint.cursor.clone();
            let replay = crate::observations::ObservationReplay::restore(checkpoint, false)?;
            replay.validate_notional_thresholds(&thresholds)?;
            replay.validate_configuration_identity(&configuration_identity)?;
            (replay, Some(cursor))
        } else {
            let replay_start_ms = inventory[channel]
                .earliest_trade_event_ms
                .unwrap_or(capture_start_ms);
            let replay = crate::observations::ObservationReplay::new(
                channel.clone(),
                configuration_identity.clone(),
                &instruments,
                60_000,
                settings.limits.dedup_identities,
                thresholds.clone(),
                replay_start_ms,
            )?;
            (replay, None)
        };
        let mut durable_output_hashes = inventory[channel].output_hashes.clone();
        let execution = replay.replay_verified_journal(
            &settings.journal_dir,
            after.as_ref(),
            settings.limits.max_replay_records,
        )?;
        for (cursor, output) in execution.outputs {
            for (instrument_id, bar) in output.time_bars {
                let payload = json!({"record_type":"time_bar","schema_version":"okx.observations.record.v1","channel":channel,"instrument_id":instrument_id,
                    "available_at_ms":bar.available_at_ms,"computed_at_ms":capture_start_ms,"published_at_ms":capture_start_ms,
                    "replay_mode":"reconstructed_after_restart","input_cursor":cursor,"bar":bar});
                let identity = derived_bar_identity(&payload)?;
                let content_hash = derived_bar_content_hash(&payload)?;
                if let Some(previous) = durable_output_hashes.get(&identity) {
                    ensure!(
                        previous == &content_hash,
                        "replayed time bar differs from durable output"
                    );
                } else {
                    journal.submit(payload)?.durable().await?;
                    durable_output_hashes.insert(identity, content_hash);
                }
            }
            for (instrument_id, bar) in output.notional_bars {
                let payload = json!({"record_type":"notional_bar","schema_version":"okx.observations.record.v1","channel":channel,"instrument_id":instrument_id,
                    "available_at_ms":bar.available_at_ms,"computed_at_ms":capture_start_ms,"published_at_ms":capture_start_ms,
                    "replay_mode":"reconstructed_after_restart","input_cursor":cursor,"bar":bar});
                let identity = derived_bar_identity(&payload)?;
                let content_hash = derived_bar_content_hash(&payload)?;
                if let Some(previous) = durable_output_hashes.get(&identity) {
                    ensure!(
                        previous == &content_hash,
                        "replayed notional bar differs from durable output"
                    );
                } else {
                    journal.submit(payload)?.durable().await?;
                    durable_output_hashes.insert(identity, content_hash);
                }
            }
        }
        if inventory[channel].has_history {
            // A process restart creates a real interval with no source coverage. Persist
            // that boundary before any new subscription can qualify quiet bars.
            for instrument in &settings.instruments {
                let gap = json!({"schema_version":"okx.observations.source-event.v1",
                    "event":"gap","reason":"PROCESS_RESTART","channel":channel,
                    "instrument_id":instrument.instrument_id,"available_at_ms":capture_start_ms,
                    "qualified_source":false});
                submit_record(&journal, &state, gap.clone()).await?;
                replay.apply(&gap)?;
            }
        }
        for calibration in &settings.notional_bars {
            if channel == "trades-all" {
                journal.submit(json!({"schema_version":"okx.observations.notional-calibration.v1",
                    "event":"notional_bar_calibration","channel":channel,"instrument_id":calibration.instrument_id,
                    "calibration_id":calibration.calibration_id,"calibration_receipt_sha256":calibration.calibration_receipt_sha256,
                    "estimator_version":calibration.estimator_version,"calibration_start_ms":calibration.calibration_start_ms,
                    "calibration_end_ms":calibration.calibration_end_ms,"scored_start_ms":calibration.scored_start_ms,
                    "quote_threshold":calibration.quote_threshold,"available_at_ms":wall_ms()?}))?.durable().await?;
            }
        }
        replay.begin_live_epoch();
        replays.insert(channel.clone(), replay);
    }
    // Do not open trade/BBO subscriptions until regional exchange metadata has
    // been observed and matched to the frozen configured rules.
    if let Err(error) = observe_metadata_once(&settings, &journal, &state).await {
        state.fail(format!("metadata qualification failed: {error:#}"));
        let mut health = state.health_snapshot()?;
        health.running = false;
        health.last_error = Some(format!("metadata qualification failed: {error:#}"));
        state.set_health(health)?;
        journal.shutdown().await?;
        return Err(error);
    }
    // The OKX contracts put trades-all on the business socket and bbo-tbt on public.
    // Keep each stream isolated even when only one of them is enabled.
    let mut tasks = JoinSet::new();
    tasks.spawn(refresh_metadata(
        settings.clone(),
        journal.clone(),
        state.clone(),
        stop.clone(),
    ));
    for channel in settings.channels.clone() {
        let (settings, journal, stop, state) = (
            settings.clone(),
            journal.clone(),
            stop.clone(),
            state.clone(),
        );
        let replay = replays
            .remove(&channel)
            .expect("replay state prepared for configured channel");
        tasks.spawn(
            async move { run_channel(settings, journal, stop, channel, state, replay).await },
        );
    }
    let failed = state.failed();
    let first = tokio::select! {
        _ = stop.cancelled() => Ok(()),
        _ = failed.cancelled() => Err(anyhow::anyhow!(state.health_snapshot()?.last_error.unwrap_or_else(|| "observation durability failed".into()))),
        result = tasks.join_next() => match result {
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(anyhow::anyhow!("observation channel task panicked: {error}")),
            None => Err(anyhow::anyhow!("observation channel tasks stopped unexpectedly")),
        }
    };
    stop.cancel();
    // A raw frame is journaled before its item dispositions. Let an in-flight
    // source branch finish that durable unit before closing the journal.
    // Stalled I/O remains bounded and is reported as an incomplete shutdown.
    let mut first = first;
    if let Err(error) = drain_capture_tasks(&mut tasks, &mut first, Duration::from_secs(120)).await
    {
        first = Err(error);
    }
    journal.shutdown().await?;
    let mut health = state.health_snapshot()?;
    health.running = false;
    if let Err(error) = &first {
        health.last_error = Some(format!("{error:#}"));
    }
    state.set_health(health)?;
    first
}

async fn run_channel(
    settings: ObservationSettings,
    journal: JournalWorker,
    stop: CancellationToken,
    channel: String,
    state: Arc<CaptureState>,
    replay: crate::observations::ObservationReplay,
) -> Result<()> {
    let mut failures = 0u32;
    let (mut dedup, mut time_bars, mut notional_bars, _) = replay.into_capture_parts();
    let capture_start_ms = wall_ms()?;
    for instrument in &settings.instruments {
        time_bars
            .entry(instrument.instrument_id.clone())
            .or_insert(FixedTimeBars::new_at(60_000, capture_start_ms)?);
    }
    for metadata in &settings.instruments {
        submit_record(
            &journal,
            &state,
            json!({"schema_version":"okx.observations.metadata.v1","event":"metadata_available",
            "venue":"okx","environment":settings.environment,"region":settings.region.as_str(),
            "instrument_id":metadata.instrument_id,"inst_id":metadata.inst_id,"metadata_revision":metadata.metadata_revision,
            "tick_size":metadata.tick_size,"lot_size":metadata.lot_size,"minimum_size":metadata.minimum_size,
            "effective_at_ms":metadata.effective_at_ms,"available_at_ms":metadata.available_at_ms}),
        ).await?;
    }
    loop {
        if stop.is_cancelled() {
            break;
        }
        let source_epoch = Uuid::new_v4().to_string();
        let connection_id = Uuid::new_v4().to_string();
        if failures > 0 && channel == "trades-all" {
            for instrument in &settings.instruments {
                if dedup.has_history_for(&instrument.instrument_id) {
                    dedup.begin_epoch_for([instrument.instrument_id.as_str()]);
                }
            }
        }
        if let Ok(mut health) = state.0.health.write() {
            health
                .source_epochs
                .insert(channel.clone(), source_epoch.clone());
        }
        // Session checks cancellation only between complete durable frame or
        // timer branches. Cancelling this future here could orphan a raw frame.
        let result = connect_and_capture(
            &settings,
            &journal,
            &stop,
            &channel,
            &source_epoch,
            &connection_id,
            &mut dedup,
            &mut time_bars,
            &mut notional_bars,
            &state,
        )
        .await;
        let reason = match result {
            Ok(()) => "source_closed",
            Err(error) => {
                if state.failed().is_cancelled() {
                    return Err(error);
                }
                eprintln!(
                    "observation capture connection failed region={} epoch={source_epoch}: {error:#}",
                    settings.region.as_str()
                );
                failures = failures.saturating_add(1);
                "source_disconnected"
            }
        };
        if reason == "source_disconnected" {
            let gap_at_ms = wall_ms()?;
            for instrument in &settings.instruments {
                submit_record(
                    &journal,
                    &state,
                    json!({"schema_version":"okx.observations.source-event.v1",
                    "event":"gap","channel":channel,"instrument_id":instrument.instrument_id,
                    "source_epoch":source_epoch,"connection_id":connection_id,"reason":"SOURCE_DISCONNECTED",
                    "available_at_ms":gap_at_ms}),
                ).await?;
                if let Some(bars) = time_bars.get_mut(&instrument.instrument_id) {
                    bars.reset_at(gap_at_ms);
                }
                if channel == "trades-all"
                    && let Some(builder) = notional_bars.get_mut(&instrument.instrument_id)
                    && builder.reset_open()
                {
                    submit_record(
                        &journal,
                        &state,
                        json!({"schema_version":"okx.observations.notional-bar.v1",
                        "event":"incomplete_bar","channel":channel,"instrument_id":instrument.instrument_id,
                        "source_epoch":source_epoch,"reason":"SOURCE_GAP","available_at_ms":gap_at_ms}),
                    ).await?;
                }
            }
        }
        submit_record(
            &journal,
            &state,
            json!({
                "schema_version":"okx.observations.source-event.v1", "event":"disconnect",
                "environment":settings.environment, "region":settings.region.as_str(),
                "channel":channel,
                "source_epoch":source_epoch, "connection_id":connection_id, "reason":reason,
                "failure_count":failures, "available_at_ms":wall_ms()?,
            }),
        )
        .await?;
        if stop.is_cancelled() {
            break;
        }
        let delay = Duration::from_secs(2u64.saturating_pow(failures.min(4)).min(30));
        tokio::select! { _ = stop.cancelled() => break, _ = sleep(delay) => {} }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Each dependency is explicit per independent source session.
async fn connect_and_capture(
    settings: &ObservationSettings,
    journal: &JournalWorker,
    stop: &CancellationToken,
    channel: &str,
    source_epoch: &str,
    connection_id: &str,
    dedup: &mut TradeDeduplicator,
    time_bars: &mut BTreeMap<String, FixedTimeBars>,
    notional_bars: &mut BTreeMap<String, crate::observations::NotionalBarBuilder>,
    state: &Arc<CaptureState>,
) -> Result<()> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(settings.limits.max_frame_bytes))
        .max_frame_size(Some(settings.limits.max_frame_bytes));
    let endpoint = websocket_endpoint(settings, channel)?;
    let (socket, _) = timeout(
        Duration::from_secs(15),
        connect_async_with_config(endpoint, Some(config), false),
    )
    .await
    .context("observation websocket connect timeout")??;
    session(
        settings,
        journal,
        stop,
        socket,
        channel,
        source_epoch,
        connection_id,
        dedup,
        time_bars,
        notional_bars,
        state,
    )
    .await
}

#[allow(clippy::too_many_arguments)] // These are explicit per-connection lineage and cancellation dependencies.
async fn session<S>(
    settings: &ObservationSettings,
    journal: &JournalWorker,
    stop: &CancellationToken,
    mut socket: WebSocketStream<S>,
    channel: &str,
    source_epoch: &str,
    connection_id: &str,
    dedup: &mut TradeDeduplicator,
    time_bars: &mut BTreeMap<String, FixedTimeBars>,
    notional_bars: &mut BTreeMap<String, crate::observations::NotionalBarBuilder>,
    state: &Arc<CaptureState>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut metadata = BTreeMap::new();
    for item in &settings.instruments {
        metadata.insert(item.inst_id.as_str(), item);
    }
    let mut requested = Vec::new();
    for instrument in &settings.instruments {
        requested.push(json!({"channel":channel,"instId":instrument.inst_id}));
    }
    ensure!(
        requested.len() <= settings.limits.max_subscriptions,
        "observation subscription bound exceeded"
    );
    timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(
            json!({"op":"subscribe","args":requested})
                .to_string()
                .into(),
        )),
    )
    .await
    .context("observation subscribe write timeout")??;
    let mut acknowledged = BTreeSet::new();
    let mut acknowledged_at_ms = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let clock_epoch = Uuid::new_v4().to_string();
    let monotonic_start = Instant::now();
    // Restored bars remain in their saved event-time buckets; only new instruments begin at now.
    let mut receive_sequence = 0u64;
    let mut bbo_progress: BTreeMap<String, BboProgress> = BTreeMap::new();
    let mut ping_at = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut pong_deadline: Option<tokio::time::Instant> = None;
    let mut heartbeat = tokio::time::interval(Duration::from_millis(250));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut bar_timer = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    bar_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = bar_timer.tick() => {
                let timer_at_ms = wall_ms()?;
                let all_acknowledged = acknowledgements_complete(&acknowledged, &requested);
                let trade_stream = channel == "trades-all";
                let mut timer_discontinuities = BTreeSet::new();
                if trade_stream {
                    for instrument in &settings.instruments {
                        if time_bars
                            .get(&instrument.instrument_id)
                            .is_some_and(|bars| bars.requires_epoch_reset(timer_at_ms))
                        {
                            timer_discontinuities.insert(instrument.instrument_id.clone());
                            submit_record(journal, state, json!({"schema_version":"okx.observations.source-event.v1",
                                "event":"gap","channel":channel,"instrument_id":instrument.instrument_id,
                                "source_epoch":source_epoch,"reason":"TIMER_DISCONTINUITY",
                                "available_at_ms":timer_at_ms})).await?;
                            if let Some(bars) = time_bars.get_mut(&instrument.instrument_id) {
                                bars.reset_at(timer_at_ms);
                            }
                            if let Some(builder) = notional_bars.get_mut(&instrument.instrument_id) {
                                builder.reset_open();
                            }
                            dedup.begin_epoch_for([instrument.instrument_id.as_str()]);
                        }
                    }
                }
                let coverage_by_instrument = settings
                    .instruments
                    .iter()
                    .map(|instrument| {
                        let interval_start = time_bars
                            .get(&instrument.instrument_id)
                            .map(FixedTimeBars::last_finalized_end_ms)
                            .unwrap_or(timer_at_ms);
                        let ack_at = acknowledged_at_ms.get(&instrument.inst_id).copied();
                        let qualified = trade_stream
                            && all_acknowledged
                            && !timer_discontinuities.contains(&instrument.instrument_id)
                            && ack_at.is_some_and(|available_at| available_at <= interval_start);
                        (
                            instrument.instrument_id.clone(),
                            json!({"qualified":qualified,"coverage_start_ms":ack_at}),
                        )
                    })
                    .collect::<serde_json::Map<_, _>>();
                let complete_coverage = coverage_by_instrument
                    .values()
                    .all(|coverage| coverage["qualified"] == true);
                submit_record(journal, state, json!({"schema_version":"okx.observations.timer.v1","event":"timer",
                    "timer_kind":if trade_stream {"fixed_time_bar"} else {"checkpoint"},"duration_ms":60_000,"source_epoch":source_epoch,
                    "channel":channel,"available_at_ms":timer_at_ms,"qualified_source":complete_coverage,
                    "coverage_by_instrument":coverage_by_instrument})).await?;
                if trade_stream {
                    for instrument in &settings.instruments {
                        let bars = time_bars.entry(instrument.instrument_id.clone()).or_insert(FixedTimeBars::new(60_000)?);
                        let pair_coverage = coverage_by_instrument[&instrument.instrument_id]["qualified"] == true;
                        for bar in bars.finalize_through(timer_at_ms, pair_coverage)? {
                            submit_record(journal, state, json!({"record_type":"time_bar","schema_version":"okx.observations.record.v1","source_epoch":source_epoch,
                                "instrument_id":instrument.instrument_id,"channel":channel,"available_at_ms":bar.available_at_ms,
                                "computed_at_ms":timer_at_ms,"bar":bar})).await?;
                        }
                    }
                }
                let checkpoint = crate::observations::ReplayCheckpoint {
                    schema_version: "okx.observations.replay-checkpoint.v1".into(),
                    channel: channel.into(),
                    cursor: JournalCursor { segment: 0, sequence: 0, hash: "0".repeat(64) },
                    bar_duration_ms: 60_000,
                    configuration_identity: settings.observation_identity()?,
                    dedup: dedup.checkpoint(),
                    time_bars: time_bars.clone(),
                    notional_bars: notional_bars.clone(),
                };
                let checkpoint_payload = json!({"record_type":"checkpoint","schema_version":"okx.observations.record.v1","source_epoch":source_epoch,
                    "channel":channel,"available_at_ms":timer_at_ms,"checkpoint":checkpoint});
                submit_checkpoint(journal, state, checkpoint_payload).await?;
            }
            _ = heartbeat.tick() => {
                let now = tokio::time::Instant::now();
                ensure_session_deadlines(
                    now,
                    acknowledgements_complete(&acknowledged, &requested),
                    deadline,
                    pong_deadline,
                )?;
                if now >= ping_at && pong_deadline.is_none() {
                    timeout(Duration::from_secs(5), socket.send(Message::Text("ping".into()))).await??;
                    ping_at = now + Duration::from_secs(20);
                    pong_deadline = Some(now + Duration::from_secs(10));
                }
            }
            incoming = socket.next() => {
                let message = incoming.context("observation websocket EOF")??;
                let raw_text = match message {
                    Message::Text(text) => text.to_string(),
                    Message::Ping(payload) => { timeout(Duration::from_secs(5), socket.send(Message::Pong(payload))).await??; continue; },
                    Message::Pong(_) => { pong_deadline = None; continue; },
                    Message::Close(_) => bail!("observation websocket closed"),
                    _ => bail!("unexpected observation websocket frame"),
                };
                if raw_text == "pong" { ensure!(pong_deadline.is_some(), "unsolicited observation pong"); pong_deadline = None; continue; }
                receive_sequence = receive_sequence.checked_add(1).context("receive sequence overflow")?;
                let receive_at_ms = wall_ms()?;
                let receive_utc_ns = wall_ns()?;
                let receive_monotonic_ns = u64::try_from(monotonic_start.elapsed().as_nanos()).context("monotonic receive time overflow")?;
                let raw_sha256 = payload_sha256(raw_text.as_bytes());
                let raw_frame_sequence = receive_sequence;
                submit_record(journal, state, json!({"schema_version":"okx.observations.raw-frame.v1",
                    "event":"raw_frame","environment":settings.environment,"region":settings.region.as_str(),
                    "source_epoch":source_epoch,"connection_id":connection_id,"channel":channel,
                    "receive_sequence":receive_sequence,"receive_utc_ns":receive_utc_ns,
                    "receive_monotonic_ns":receive_monotonic_ns,"clock_epoch":clock_epoch,
                    "available_at_ms":receive_at_ms,"raw_sha256":raw_sha256,"raw_text":raw_text})).await?;
                let frame: Value = match serde_json::from_str(&raw_text) {
                    Ok(frame) => frame,
                    Err(error) => {
                        submit_record(journal, state, rejection_payload(channel, source_epoch,
                            raw_frame_sequence, &raw_sha256, receive_at_ms, "INVALID_JSON")).await?;
                        return Err(error).context("invalid observation JSON frame");
                    }
                };
                if frame.get("event").is_some() {
                    if frame["event"] != "subscribe" {
                        submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "UNEXPECTED_SOURCE_EVENT")).await?;
                        bail!("unexpected observation source event");
                    }
                    let Some(frame_channel) = frame.get("arg").and_then(|arg| arg.get("channel")).and_then(Value::as_str) else {
                        submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "ACK_CHANNEL_MISSING")).await?;
                        bail!("subscription ack channel missing");
                    };
                    let Some(inst_id) = frame.get("arg").and_then(|arg| arg.get("instId")).and_then(Value::as_str) else {
                        submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "ACK_INSTRUMENT_MISSING")).await?;
                        bail!("subscription ack instrument missing");
                    };
                    if frame_channel != channel || !metadata.contains_key(inst_id) {
                        submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "UNREQUESTED_ACK")).await?;
                        bail!("acknowledged unrequested subscription");
                    }
                    acknowledged.insert((channel.to_string(), inst_id.to_string()));
                    acknowledged_at_ms.insert(inst_id.to_string(), receive_at_ms);
                    submit_record(journal, state, json!({"schema_version":"okx.observations.source-event.v1","event":"subscription_ack",
                        "environment":settings.environment,"region":settings.region.as_str(),"source_epoch":source_epoch,
                        "connection_id":connection_id,"channel":channel,"inst_id":inst_id,"available_at_ms":receive_at_ms,
                        "receive_sequence":receive_sequence,"raw_sha256":raw_sha256,"raw_payload":frame})).await?;
                    continue;
                }
                let Some(frame_channel) = frame.get("arg").and_then(|arg| arg.get("channel")).and_then(Value::as_str) else {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "CHANNEL_MISSING")).await?;
                    bail!("observation channel missing");
                };
                let Some(inst_id) = frame.get("arg").and_then(|arg| arg.get("instId")).and_then(Value::as_str) else {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "INSTRUMENT_MISSING")).await?;
                    bail!("observation instrument missing");
                };
                let Some(instrument) = metadata.get(inst_id) else {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "INSTRUMENT_OUTSIDE_CATALOGUE")).await?;
                    bail!("observation instrument outside configured catalogue");
                };
                if frame_channel != channel {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "UNREQUESTED_CHANNEL")).await?;
                    bail!("received unrequested observation channel");
                }
                let Some(data) = frame.get("data").and_then(Value::as_array) else {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "DATA_NOT_ARRAY")).await?;
                    bail!("observation data must be an array");
                };
                if data.is_empty() || data.len() > 1000 {
                    submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "INVALID_BATCH_SIZE")).await?;
                    bail!("invalid observation batch size");
                }
                let acked = acknowledged.contains(&(channel.to_string(), inst_id.to_string()));
                for (payload_index, payload) in data.iter().enumerate() {
                    let mut reasons = Vec::new();
                    if !acked { reasons.push("DATA_BEFORE_ACK"); }
                    let Some(event_at_ms) = payload.get("ts").and_then(Value::as_str).and_then(|v| v.parse::<u64>().ok()) else {
                        submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "TIMESTAMP_INVALID")).await?;
                        reject_remaining_batch(journal, state, RejectBatch {
                            channel, source_epoch, raw_frame_sequence, raw_sha256: &raw_sha256,
                            available_at_ms: receive_at_ms,
                        }, data.len().saturating_sub(payload_index + 1)).await?;
                        bail!("observation timestamp invalid");
                    };
                    if instrument.available_at_ms > receive_at_ms || instrument.effective_at_ms > event_at_ms {
                        reasons.push("METADATA_UNAVAILABLE");
                    }
                    if event_at_ms > receive_at_ms { reasons.push("FUTURE_EVENT_CLOCK"); }
                    if channel == "trades-all"
                        && !dedup.event_is_after_retention_floor(&instrument.instrument_id, event_at_ms)
                    {
                        reasons.push("DEDUP_RETENTION_FLOOR");
                    }
                    if acknowledged_at_ms
                        .get(inst_id)
                        .is_some_and(|ack_at| event_at_ms < *ack_at)
                    {
                        reasons.push("EVENT_BEFORE_COVERAGE_START");
                    }
                    if channel == "trades-all" {
                        let mut trade = match parse_trade_payload(payload, &instrument.instrument_id, channel, receive_at_ms) {
                            Ok(trade) => trade,
                            Err(error) => {
                                submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "TRADE_INVALID")).await?;
                                reject_remaining_batch(journal, state, RejectBatch {
                                    channel, source_epoch, raw_frame_sequence, raw_sha256: &raw_sha256,
                                    available_at_ms: receive_at_ms,
                                }, data.len().saturating_sub(payload_index + 1)).await?;
                                return Err(error).context("invalid trade payload");
                            }
                        };
                        trade.environment = settings.environment.clone();
                        trade.region = settings.region.as_str().into();
                        trade.metadata_revision = instrument.metadata_revision.clone();
                        trade.source_epoch = source_epoch.into();
                        trade.connection_id = connection_id.into();
                        trade.receive_sequence = receive_sequence;
                        trade.raw_sha256 = raw_sha256.clone();
                        trade.reason_codes = reasons.iter().map(|r| (*r).to_string()).collect();
                        trade.quality = if trade.reason_codes.is_empty() { "qualified" } else { "unqualified" }.into();
                        if trade.quality != "qualified" {
                            time_bars
                                .get_mut(&instrument.instrument_id)
                                .context("fixed bar state missing")?
                                .invalidate_event_time(trade.event_at_ms, "UNQUALIFIED_TRADE");
                        }
                        let decision = match dedup.preview(&trade) {
                            Ok(decision) => decision,
                            Err(error) => {
                                submit_record(journal, state, json!({"schema_version":"okx.observations.dedup.v1",
                                    "event":"trade_conflict","instrument_id":instrument.instrument_id,"channel":channel,
                                    "trade_id":trade.trade_id,"source_epoch":source_epoch,"raw_frame_sequence":raw_frame_sequence,
                                    "available_at_ms":receive_at_ms})).await?;
                                reject_remaining_batch(journal, state, RejectBatch {
                                    channel, source_epoch, raw_frame_sequence, raw_sha256: &raw_sha256,
                                    available_at_ms: receive_at_ms,
                                }, data.len().saturating_sub(payload_index + 1)).await?;
                                state.fail(format!("conflicting duplicate trade: {error:#}"));
                                return Err(error);
                            }
                        };
                        if decision == DedupDecision::Duplicate {
                            submit_record(journal, state, json!({"schema_version":"okx.observations.dedup.v1","event":"duplicate",
                                "instrument_id":instrument.instrument_id,"channel":channel,"trade_id":trade.trade_id,
                                "source_epoch":source_epoch,"raw_frame_sequence":raw_frame_sequence,
                                "available_at_ms":receive_at_ms})).await?;
                            dedup.admit(trade)?;
                            continue;
                        }
                        submit_record(journal, state, json!({"record_type":"trade","schema_version":"okx.observations.record.v1",
                            "venue":"okx","environment":settings.environment,"region":settings.region.as_str(),
                            "instrument_id":instrument.instrument_id,"channel":channel,"source_epoch":source_epoch,
                            "connection_id":connection_id,"receive_sequence":receive_sequence,"receive_utc_ns":receive_utc_ns,
                            "receive_monotonic_ns":receive_monotonic_ns,"clock_epoch":clock_epoch,
                            "available_at_ms":receive_at_ms,"metadata_revision":instrument.metadata_revision,
                            "raw_sha256":raw_sha256,"raw_frame_sequence":raw_frame_sequence,"observation":trade})).await?;
                        ensure!(dedup.admit(trade.clone())? == DedupDecision::New,
                            "trade dedup classification changed after journal submission");
                        let late_for_bar = trade.quality == "qualified"
                            && !time_bars.get_mut(&instrument.instrument_id).context("fixed bar state missing")?.push(trade.clone())?;
                        if late_for_bar {
                            submit_record(journal, state, json!({"schema_version":"okx.observations.late-event.v1","event":"late_for_finalized_bar",
                                "instrument_id":instrument.instrument_id,"channel":channel,"source_epoch":source_epoch,
                                "trade_id":payload["tradeId"],"raw_frame_sequence":raw_frame_sequence,
                                "available_at_ms":receive_at_ms})).await?;
                        }
                        if trade.quality == "qualified"
                            && let Some(builder) = notional_bars.get_mut(&instrument.instrument_id)
                        {
                            for bar in builder.push(trade.clone())? {
                                let computed_at_ms = wall_ms()?;
                                submit_record(journal, state, json!({"record_type":"notional_bar",
                                    "schema_version":"okx.observations.record.v1","channel":channel,
                                    "instrument_id":instrument.instrument_id,"source_epoch":source_epoch,
                                    "metadata_revision":instrument.metadata_revision,
                                    "available_at_ms":bar.available_at_ms,"computed_at_ms":computed_at_ms,"bar":bar})).await?;
                            }
                        }
                    } else {
                        let mut bbo = match parse_bbo_payload(payload, &instrument.instrument_id, settings.region.as_str(),
                            &instrument.metadata_revision, source_epoch, receive_at_ms) {
                            Ok(bbo) => bbo,
                            Err(error) => {
                                submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "BBO_INVALID")).await?;
                                reject_remaining_batch(journal, state, RejectBatch {
                                    channel, source_epoch, raw_frame_sequence, raw_sha256: &raw_sha256,
                                    available_at_ms: receive_at_ms,
                                }, data.len().saturating_sub(payload_index + 1)).await?;
                                return Err(error).context("invalid BBO payload");
                            }
                        };
                        bbo.environment = settings.environment.clone();
                        let mut bbo_reasons: BTreeSet<String> =
                            bbo.reason_codes.iter().cloned().collect();
                        bbo_reasons.extend(reasons.iter().map(|reason| (*reason).to_string()));
                        let progress_issue = advance_bbo_progress(&mut bbo_progress, &bbo);
                        if let Some(reason) = progress_issue {
                            bbo_reasons.insert(reason.into());
                        }
                        if progress_issue == Some("BBO_SEQUENCE_REPEAT") {
                            submit_record(journal, state, json!({"schema_version":"okx.observations.dedup.v1",
                                "event":"bbo_repeat","instrument_id":instrument.instrument_id,"channel":channel,
                                "sequence":bbo.sequence,"source_epoch":source_epoch,"raw_frame_sequence":raw_frame_sequence,
                                "available_at_ms":receive_at_ms,"event_at_ms":bbo.event_at_ms})).await?;
                            continue;
                        }
                        let fatal_bbo_progress_issue = progress_issue
                            .is_some_and(|reason| reason != "BBO_SEQUENCE_REPEAT");
                        bbo.reason_codes = bbo_reasons.into_iter().collect();
                        bbo.quality = if bbo.reason_codes.is_empty() { "qualified" } else { "unqualified" }.into();
                        if fatal_bbo_progress_issue {
                            submit_record(journal, state, rejection_payload(channel, source_epoch, raw_frame_sequence, &raw_sha256, receive_at_ms, "BBO_SEQUENCE_OR_EVENT_REGRESSION")).await?;
                            submit_record(journal, state, json!({"schema_version":"okx.observations.source-event.v1",
                                "event":"gap","instrument_id":instrument.instrument_id,"channel":channel,
                                "source_epoch":source_epoch,"reason":"BBO_SEQUENCE_OR_EVENT_REGRESSION",
                                "available_at_ms":receive_at_ms})).await?;
                            reject_remaining_batch(journal, state, RejectBatch {
                                channel, source_epoch, raw_frame_sequence, raw_sha256: &raw_sha256,
                                available_at_ms: receive_at_ms,
                            }, data.len().saturating_sub(payload_index + 1)).await?;
                            bail!("BBO sequence or event time regressed/conflicted; starting a new source epoch");
                        }
                        submit_record(journal, state, json!({"record_type":"bbo","schema_version":"okx.observations.record.v1",
                            "venue":"okx","environment":settings.environment,"region":settings.region.as_str(),
                            "instrument_id":instrument.instrument_id,"channel":channel,"source_epoch":source_epoch,
                            "connection_id":connection_id,"receive_sequence":receive_sequence,"receive_utc_ns":receive_utc_ns,
                            "receive_monotonic_ns":receive_monotonic_ns,"clock_epoch":clock_epoch,
                            "available_at_ms":receive_at_ms,"metadata_revision":instrument.metadata_revision,
                            "raw_sha256":raw_sha256,"raw_frame_sequence":raw_frame_sequence,"observation":bbo})).await?;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BboProgress {
    sequence: u64,
    event_at_ms: u64,
    bid_price: String,
    bid_quantity: String,
    ask_price: String,
    ask_quantity: String,
}

fn advance_bbo_progress(
    progress: &mut BTreeMap<String, BboProgress>,
    bbo: &crate::observations::BboSnapshot,
) -> Option<&'static str> {
    let Some(sequence) = bbo
        .sequence
        .as_deref()
        .and_then(|value| value.parse::<u64>().ok())
    else {
        return Some("BBO_SEQUENCE_MISSING");
    };
    let current = BboProgress {
        sequence,
        event_at_ms: bbo.event_at_ms,
        bid_price: bbo.bid_price.clone(),
        bid_quantity: bbo.bid_quantity.clone(),
        ask_price: bbo.ask_price.clone(),
        ask_quantity: bbo.ask_quantity.clone(),
    };
    if let Some(previous) = progress.get(&bbo.instrument_id) {
        if current.sequence < previous.sequence {
            return Some("BBO_SEQUENCE_REGRESSION");
        }
        if current.event_at_ms < previous.event_at_ms {
            return Some("BBO_EVENT_TIME_REGRESSION");
        }
        if current.sequence == previous.sequence
            && (current.bid_price != previous.bid_price
                || current.bid_quantity != previous.bid_quantity
                || current.ask_price != previous.ask_price
                || current.ask_quantity != previous.ask_quantity)
        {
            return Some("BBO_SEQUENCE_CONFLICT");
        }
        if current.sequence == previous.sequence {
            return Some("BBO_SEQUENCE_REPEAT");
        }
        if current.sequence > previous.sequence || current.event_at_ms > previous.event_at_ms {
            progress.insert(bbo.instrument_id.clone(), current);
        }
    } else {
        progress.insert(bbo.instrument_id.clone(), current);
    }
    None
}

async fn submit_record(
    journal: &JournalWorker,
    state: &Arc<CaptureState>,
    payload: Value,
) -> Result<crate::observation_journal::DurableRecord> {
    let accepted_at_ms = wall_ms()?;
    let pending = match journal.submit(payload.clone()) {
        Ok(pending) => pending,
        Err(error) => {
            state.fail(format!("journal submission rejected: {error:#}"));
            return Err(error);
        }
    };
    publish_durable(state, payload, pending, accepted_at_ms).await
}

async fn submit_checkpoint(
    journal: &JournalWorker,
    state: &Arc<CaptureState>,
    payload: Value,
) -> Result<crate::observation_journal::DurableRecord> {
    let accepted_at_ms = wall_ms()?;
    let pending = match journal.submit_checkpoint(payload.clone()) {
        Ok(pending) => pending,
        Err(error) => {
            state.fail(format!("journal checkpoint rejected: {error:#}"));
            return Err(error);
        }
    };
    publish_durable(state, payload, pending, accepted_at_ms).await
}

async fn publish_durable(
    state: &Arc<CaptureState>,
    payload: Value,
    pending: crate::observation_journal::PendingRecord,
    accepted_at_ms: u64,
) -> Result<crate::observation_journal::DurableRecord> {
    let epoch = payload
        .get("source_epoch")
        .and_then(Value::as_str)
        .unwrap_or("capture-global")
        .to_string();
    match pending.durable().await {
        Ok(record) => {
            let durable_acknowledged_at_ms = wall_ms()?;
            let published_at_ms = wall_ms()?;
            state.publish(update_from_payload(
                &record.payload,
                epoch,
                Some(record.cursor.clone()),
                accepted_at_ms,
                durable_acknowledged_at_ms,
                published_at_ms,
            ))?;
            Ok(record)
        }
        Err(error) => {
            state.fail(format!("journal durability failed: {error:#}"));
            Err(error)
        }
    }
}

fn update_from_payload(
    payload: &Value,
    epoch: String,
    durable_cursor: Option<JournalCursor>,
    accepted_at_ms: u64,
    durable_acknowledged_at_ms: u64,
    published_at_ms: u64,
) -> ObservationUpdate {
    let observation = payload.get("observation").unwrap_or(payload);
    let reason_codes = observation
        .get("reason_codes")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let instrument_id = payload
        .get("instrument_id")
        .and_then(Value::as_str)
        .or_else(|| observation.get("instrument_id").and_then(Value::as_str))
        .map(str::to_owned)
        .or_else(|| {
            payload
                .get("inst_id")
                .and_then(Value::as_str)
                .filter(|pair| pair.ends_with("-USDC"))
                .map(|pair| format!("okx:spot:{pair}"))
        });
    ObservationUpdate {
        schema_version: "okx.observations.update.v1".into(),
        sequence: 0,
        epoch,
        record_kind: payload
            .get("record_type")
            .and_then(Value::as_str)
            .or_else(|| payload.get("event").and_then(Value::as_str))
            .unwrap_or("observation")
            .into(),
        channel: payload
            .get("channel")
            .and_then(Value::as_str)
            .or_else(|| observation.get("channel").and_then(Value::as_str))
            .map(str::to_owned),
        instrument_id,
        available_at_ms: payload
            .get("available_at_ms")
            .and_then(Value::as_u64)
            .or_else(|| observation.get("available_at_ms").and_then(Value::as_u64)),
        accepted_at_ms: Some(accepted_at_ms),
        durable_acknowledged_at_ms: Some(durable_acknowledged_at_ms),
        computed_at_ms: payload.get("computed_at_ms").and_then(Value::as_u64),
        published_at_ms: Some(published_at_ms),
        quality: observation
            .get("quality")
            .and_then(Value::as_str)
            .unwrap_or("raw")
            .to_owned(),
        reason_codes,
        payload: payload.clone(),
        durable_cursor,
    }
}

struct RejectBatch<'a> {
    channel: &'a str,
    source_epoch: &'a str,
    raw_frame_sequence: u64,
    raw_sha256: &'a str,
    available_at_ms: u64,
}

async fn reject_remaining_batch(
    journal: &JournalWorker,
    state: &Arc<CaptureState>,
    batch: RejectBatch<'_>,
    count: usize,
) -> Result<()> {
    for _ in 0..count {
        submit_record(
            journal,
            state,
            rejection_payload(
                batch.channel,
                batch.source_epoch,
                batch.raw_frame_sequence,
                batch.raw_sha256,
                batch.available_at_ms,
                "BATCH_ABORTED_AFTER_INVALID_ITEM",
            ),
        )
        .await?;
    }
    Ok(())
}

fn rejection_payload(
    channel: &str,
    source_epoch: &str,
    raw_frame_sequence: u64,
    raw_sha256: &str,
    available_at_ms: u64,
    reason: &str,
) -> Value {
    json!({"schema_version":"okx.observations.rejection.v1","event":"parse_rejection",
        "channel":channel,"source_epoch":source_epoch,"raw_frame_sequence":raw_frame_sequence,
        "raw_sha256":raw_sha256,"reason":reason,"available_at_ms":available_at_ms})
}

fn acknowledgements_complete(
    acknowledged: &BTreeSet<(String, String)>,
    requested: &[Value],
) -> bool {
    requested.iter().all(|item| {
        let channel = item
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let instrument = item
            .get("instId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        acknowledged.contains(&(channel.to_string(), instrument.to_string()))
    })
}

fn ensure_session_deadlines(
    now: tokio::time::Instant,
    acknowledgements_complete: bool,
    acknowledgement_deadline: tokio::time::Instant,
    pong_deadline: Option<tokio::time::Instant>,
) -> Result<()> {
    ensure!(
        acknowledgements_complete || now < acknowledgement_deadline,
        "observation subscription acknowledgement timeout"
    );
    ensure!(
        pong_deadline.is_none_or(|deadline| now < deadline),
        "observation pong timeout"
    );
    Ok(())
}

fn wall_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("wall clock before epoch")?
            .as_millis(),
    )
    .context("wall clock milliseconds overflow")
}
fn wall_ns() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("wall clock before epoch")?
            .as_nanos(),
    )
    .context("wall clock nanoseconds overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        observation_config::{InstrumentMetadata, ObservationLimits, Region},
        observation_journal::read_verified_records,
    };
    use tokio::{net::TcpListener, time::sleep};
    use tokio_tungstenite::{accept_async, connect_async};

    fn bbo(sequence: &str, event_at_ms: u64, bid: &str) -> crate::observations::BboSnapshot {
        crate::observations::BboSnapshot {
            schema_version: "okx.observations.v1".into(),
            venue: "okx".into(),
            environment: "public".into(),
            region: "global".into(),
            instrument_id: "okx:spot:BTC-USDC".into(),
            channel: "bbo-tbt".into(),
            bid_price: bid.into(),
            bid_quantity: "2".into(),
            ask_price: "101".into(),
            ask_quantity: "3".into(),
            sequence: Some(sequence.into()),
            event_at_ms,
            available_at_ms: event_at_ms,
            metadata_revision: "fixture-meta".into(),
            source_epoch: "fixture-epoch".into(),
            snapshot: true,
            depth_scope: "single_level".into(),
            quality: "raw".into(),
            reason_codes: Vec::new(),
        }
    }

    #[test]
    fn bbo_progress_rejects_regression_and_same_sequence_conflict_but_allows_repeat() {
        let mut progress = BTreeMap::new();
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("10", 100, "99")),
            None
        );
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("10", 100, "99")),
            Some("BBO_SEQUENCE_REPEAT")
        );
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("10", 100, "98")),
            Some("BBO_SEQUENCE_CONFLICT")
        );
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("9", 101, "99")),
            Some("BBO_SEQUENCE_REGRESSION")
        );
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("11", 99, "99")),
            Some("BBO_EVENT_TIME_REGRESSION")
        );
        assert_eq!(
            advance_bbo_progress(&mut progress, &bbo("11", 101, "99")),
            None
        );
    }

    #[test]
    fn busy_session_still_fails_missing_pong_and_missing_ack_deadlines() {
        let start = tokio::time::Instant::now();
        let ack_deadline = start + Duration::from_secs(20);
        assert!(
            ensure_session_deadlines(start + Duration::from_secs(19), false, ack_deadline, None)
                .is_ok()
        );
        assert!(
            ensure_session_deadlines(start + Duration::from_secs(21), false, ack_deadline, None)
                .is_err()
        );
        let pong_deadline = start + Duration::from_secs(10);
        assert!(
            ensure_session_deadlines(
                start + Duration::from_secs(9),
                true,
                ack_deadline,
                Some(pong_deadline)
            )
            .is_ok()
        );
        // Data traffic does not alter this deadline; only a Pong clears it in the session loop.
        assert!(
            ensure_session_deadlines(
                start + Duration::from_secs(10),
                true,
                ack_deadline,
                Some(pong_deadline)
            )
            .is_err()
        );
    }

    fn settings(journal_dir: String) -> ObservationSettings {
        ObservationSettings {
            schema_version: "okx.observations.config.v1".into(),
            enabled: true,
            environment: "public".into(),
            region: Region::Global,
            instruments: vec![InstrumentMetadata {
                inst_id: "BTC-USDC".into(),
                instrument_id: "okx:spot:BTC-USDC".into(),
                metadata_revision: "fixture-meta-v1".into(),
                tick_size: "0.1".into(),
                lot_size: "0.01".into(),
                minimum_size: "0.01".into(),
                effective_at_ms: 1,
                available_at_ms: 1,
            }],
            notional_bars: vec![],
            channels: vec!["bbo-tbt".into()],
            limits: ObservationLimits {
                max_pairs: 1,
                max_sockets: 1,
                max_subscriptions: 1,
                max_frame_bytes: 64 * 1024,
                queue_capacity: 16,
                dedup_identities: 64,
                max_replay_records: 10_000,
                metadata_refresh_interval_ms: 60_000,
                segment_bytes: 16 * 1024 * 1024,
                minimum_disk_free_bytes: 1,
                raw_retention_days: 1,
            },
            journal_dir,
        }
    }

    #[test]
    fn trade_and_bbo_channels_use_distinct_allowlisted_socket_endpoints() {
        let config = settings("/tmp/unused".into());
        assert_eq!(
            websocket_endpoint(&config, "trades-all").unwrap(),
            "wss://ws.okx.com/ws/v5/business"
        );
        assert_eq!(
            websocket_endpoint(&config, "bbo-tbt").unwrap(),
            "wss://ws.okx.com/ws/v5/public"
        );
        assert!(websocket_endpoint(&config, "books").is_err());
    }

    #[test]
    fn missing_acknowledgement_is_not_complete_until_each_requested_pair_is_acknowledged() {
        let requested = vec![
            json!({"channel":"bbo-tbt","instId":"BTC-USDC"}),
            json!({"channel":"bbo-tbt","instId":"ETH-USDC"}),
        ];
        let mut acknowledged = BTreeSet::new();
        assert!(!acknowledgements_complete(&acknowledged, &requested));
        acknowledged.insert(("bbo-tbt".into(), "BTC-USDC".into()));
        assert!(!acknowledgements_complete(&acknowledged, &requested));
        acknowledged.insert(("bbo-tbt".into(), "ETH-USDC".into()));
        assert!(acknowledgements_complete(&acknowledged, &requested));
    }

    #[test]
    fn concurrent_publish_keeps_broadcast_and_health_sequences_monotonic() {
        let mut config = settings("/tmp/unused".into());
        config.limits.queue_capacity = 128;
        let state = CaptureState::new(&config).unwrap();
        let mut updates = state.subscribe();
        let mut workers = Vec::new();
        for index in 0..64 {
            let state = state.clone();
            workers.push(std::thread::spawn(move || {
                state
                    .publish(ObservationUpdate {
                        schema_version: String::new(),
                        sequence: 0,
                        epoch: format!("epoch-{index}"),
                        record_kind: "timer".into(),
                        channel: None,
                        instrument_id: None,
                        available_at_ms: Some(index),
                        accepted_at_ms: None,
                        durable_acknowledged_at_ms: None,
                        computed_at_ms: None,
                        published_at_ms: None,
                        quality: "raw".into(),
                        reason_codes: Vec::new(),
                        payload: json!({"event":"timer"}),
                        durable_cursor: None,
                    })
                    .unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        let received = (0..64)
            .map(|_| updates.try_recv().unwrap().sequence)
            .collect::<Vec<_>>();
        assert!(received.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(state.health_snapshot().unwrap().last_update_sequence, 64);
        assert_eq!(
            state.health_snapshot().unwrap().last_available_at_ms,
            Some(63)
        );
    }

    #[test]
    fn raw_frames_require_a_normalized_or_rejection_disposition() {
        let entry = |payload| crate::observation_journal::JournalEntry {
            cursor: JournalCursor {
                segment: 0,
                sequence: 1,
                hash: "a".repeat(64),
            },
            payload,
        };
        let raw = entry(json!({"event":"raw_frame","channel":"trades-all","receive_sequence":7}));
        assert!(validate_raw_frame_dispositions(std::slice::from_ref(&raw)).is_err());
        let rejection =
            entry(json!({"event":"parse_rejection","channel":"trades-all","raw_frame_sequence":7}));
        assert!(validate_raw_frame_dispositions(&[raw, rejection]).is_ok());
    }

    #[test]
    fn derived_late_event_cannot_count_as_a_raw_item_disposition() {
        let entry = |payload| crate::observation_journal::JournalEntry {
            cursor: JournalCursor {
                segment: 0,
                sequence: 1,
                hash: "a".repeat(64),
            },
            payload,
        };
        let raw = entry(
            json!({"event":"raw_frame","channel":"trades-all","receive_sequence":7,
            "raw_text":"{\"data\":[{},{}]}"}),
        );
        let late = entry(
            json!({"event":"late_for_finalized_bar","channel":"trades-all","raw_frame_sequence":7}),
        );
        assert!(validate_raw_frame_dispositions(&[raw, late]).is_err());
    }

    #[test]
    fn in_memory_validator_counts_source_event_once_even_with_data() {
        let entry = |payload| crate::observation_journal::JournalEntry {
            cursor: JournalCursor {
                segment: 0,
                sequence: 1,
                hash: "a".repeat(64),
            },
            payload,
        };
        let raw = entry(json!({"event":"raw_frame","channel":"bbo-tbt",
            "source_epoch":"fixture","receive_sequence":9,
            "raw_text":"{\"event\":\"subscribe\",\"data\":[{},{}]}"}));
        let ack = entry(json!({"event":"subscription_ack","channel":"bbo-tbt",
            "source_epoch":"fixture","receive_sequence":9}));
        assert!(validate_raw_frame_dispositions(&[raw, ack]).is_ok());
    }

    async fn rejected_source_frame_keeps_replayable_journal(channel: &str, frames: Vec<Value>) {
        let root = std::env::temp_dir().join(format!("observation-rejection-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let mut settings = settings(root.to_string_lossy().into_owned());
        settings.channels = vec![channel.into()];
        let state = CaptureState::new(&settings).unwrap();
        let journal = JournalWorker::start(&root, 16 * 1024, 16).unwrap();
        let stop = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let _ = socket.next().await.unwrap().unwrap();
            for frame in frames {
                socket
                    .send(Message::Text(frame.to_string().into()))
                    .await
                    .unwrap();
            }
        });
        let (client, _) = connect_async(format!("ws://{address}")).await.unwrap();
        let mut dedup = TradeDeduplicator::new(64).unwrap();
        let mut time_bars = BTreeMap::from([(
            "okx:spot:BTC-USDC".to_string(),
            FixedTimeBars::new_at(60_000, 0).unwrap(),
        )]);
        let mut notional_bars = BTreeMap::new();
        assert!(
            session(
                &settings,
                &journal,
                &stop,
                client,
                channel,
                "fixture-epoch",
                "fixture-connection",
                &mut dedup,
                &mut time_bars,
                &mut notional_bars,
                &state,
            )
            .await
            .is_err()
        );
        server.await.unwrap();
        journal.shutdown().await.unwrap();
        let entries = read_verified_records(&root).unwrap();
        if channel == "trades-all" {
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.payload["event"] == "trade_conflict")
            );
        }
        assert!(
            validate_raw_frame_dispositions_stream(&root, 100).is_ok(),
            "a rejected raw frame must have a complete durable disposition"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn unexpected_frame_channel_rejection_preserves_raw_lineage() {
        rejected_source_frame_keeps_replayable_journal(
            "bbo-tbt",
            vec![json!({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDC"}})],
        )
        .await;
        rejected_source_frame_keeps_replayable_journal(
            "bbo-tbt",
            vec![json!({"arg":{"channel":"books","instId":"BTC-USDC"},"data":[{}]})],
        )
        .await;
    }

    #[tokio::test]
    async fn trade_conflict_disposes_remaining_frame_items_before_stop() {
        let ts = wall_ms().unwrap().saturating_sub(1).to_string();
        rejected_source_frame_keeps_replayable_journal(
            "trades-all",
            vec![
                json!({"arg":{"channel":"trades-all","instId":"BTC-USDC"},"data":[
                    {"instId":"BTC-USDC","tradeId":"777","px":"100","sz":"1","side":"buy","ts":ts},
                    {"instId":"BTC-USDC","tradeId":"777","px":"101","sz":"1","side":"buy","ts":ts},
                    {"instId":"BTC-USDC","tradeId":"778","px":"100","sz":"1","side":"buy","ts":ts}
                ]}),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn source_event_data_array_is_one_frame_disposition() {
        rejected_source_frame_keeps_replayable_journal(
            "bbo-tbt",
            vec![
                json!({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDC"},
                "data":[{},{}]}),
            ],
        )
        .await;
        rejected_source_frame_keeps_replayable_journal(
            "bbo-tbt",
            vec![json!({"event":"error","data":[{},{}]})],
        )
        .await;
        rejected_source_frame_keeps_replayable_journal(
            "bbo-tbt",
            vec![
                json!({"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDC"},
                    "data":[{},{}]}),
                json!({"event":"error","data":[{},{}]}),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn graceful_stop_drains_an_in_flight_raw_frame_disposition() {
        let root = std::env::temp_dir().join(format!("observation-stop-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let journal = JournalWorker::start(&root, 16 * 1024, 16).unwrap();
        journal
            .submit(json!({"event":"raw_frame","channel":"trades-all",
                "source_epoch":"fixture-epoch","receive_sequence":1,"raw_text":"bad JSON"}))
            .unwrap()
            .durable()
            .await
            .unwrap();
        let (release, wait_for_release) = tokio::sync::oneshot::channel::<()>();
        let worker = journal.clone();
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            wait_for_release.await?;
            worker
                .submit(json!({"event":"parse_rejection","channel":"trades-all",
                    "source_epoch":"fixture-epoch","raw_frame_sequence":1,
                    "reason":"INVALID_JSON"}))?
                .durable()
                .await?;
            Ok(())
        });
        let stop = CancellationToken::new();
        stop.cancel();
        let drain = tokio::spawn(async move {
            let mut first = Ok(());
            drain_capture_tasks(&mut tasks, &mut first, Duration::from_secs(1)).await?;
            first
        });
        sleep(Duration::from_millis(20)).await;
        assert!(
            !drain.is_finished(),
            "shutdown must await the durable disposition"
        );
        release.send(()).unwrap();
        drain.await.unwrap().unwrap();
        journal.shutdown().await.unwrap();
        validate_raw_frame_dispositions_stream(&root, 100).unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn local_websocket_ack_ping_and_bbo_are_journaled_with_durable_updates() {
        let root = std::env::temp_dir().join(format!("observation-ws-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let settings = settings(root.to_string_lossy().into_owned());
        let state = CaptureState::new(&settings).unwrap();
        let mut updates = state.subscribe();
        let journal = JournalWorker::start(&root, 16 * 1024, 16).unwrap();
        let stop = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let subscribe = socket.next().await.unwrap().unwrap().into_text().unwrap();
            assert!(subscribe.contains("subscribe"));
            socket
                .send(Message::Text(
                    json!({"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDC"}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            socket.send(Message::Ping(vec![3, 7].into())).await.unwrap();
            assert!(
                matches!(socket.next().await.unwrap().unwrap(), Message::Pong(data) if data.as_ref() == [3, 7])
            );
            let now = wall_ms().unwrap();
            socket.send(Message::Text(json!({"arg":{"channel":"bbo-tbt","instId":"BTC-USDC"},"data":[
                {"ts":now.to_string(),"bids":[["99.9","2","0",1]],"asks":[["100.1","3","0",1]],"seqId":55}
            ]}).to_string().into())).await.unwrap();
            socket.send(Message::Text(json!({"arg":{"channel":"bbo-tbt","instId":"BTC-USDC"},"data":[
                {"ts":now.to_string(),"bids":[["99.9","2","0",1]],"asks":[["100.1","3","0",1]],"seqId":55}
            ]}).to_string().into())).await.unwrap();
            sleep(Duration::from_secs(1)).await;
        });
        let (client, _) = connect_async(format!("ws://{address}")).await.unwrap();
        let mut dedup = TradeDeduplicator::new(64).unwrap();
        let mut time_bars = BTreeMap::from([(
            "okx:spot:BTC-USDC".to_string(),
            FixedTimeBars::new_at(60_000, 0).unwrap(),
        )]);
        let mut notional_bars = BTreeMap::new();
        let mut result = Box::pin(session(
            &settings,
            &journal,
            &stop,
            client,
            "bbo-tbt",
            "offline-epoch",
            "offline-conn",
            &mut dedup,
            &mut time_bars,
            &mut notional_bars,
            &state,
        ));
        tokio::select! {
            result = &mut result => panic!("local websocket should remain open: {result:?}"),
            _ = sleep(Duration::from_millis(100)) => {}
        }
        stop.cancel();
        assert!(result.as_mut().await.is_ok());
        drop(result);
        server.await.unwrap();
        journal.shutdown().await.unwrap();
        let entries = read_verified_records(&root).unwrap();
        assert!(
            entries
                .iter()
                .any(|entry| entry.payload["event"] == "subscription_ack")
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.payload["event"] == "raw_frame"
                    && entry.payload["raw_text"].is_string())
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.payload["record_type"] == "bbo")
                .count(),
            1
        );
        assert!(
            entries
                .iter()
                .any(|entry| entry.payload["event"] == "bbo_repeat")
        );
        let received = timeout(Duration::from_secs(2), async {
            let mut durable = 0;
            while durable < 3 {
                let update = updates.recv().await.unwrap();
                durable += usize::from(update.durable_cursor.is_some());
            }
        })
        .await;
        assert!(received.is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }
}
