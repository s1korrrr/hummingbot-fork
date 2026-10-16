//! Opt-in raw observation contracts and deterministic bar arithmetic.
//!
//! These types are separate from the candle and full-depth market-picture contracts.  In
//! particular a `BboSnapshot` is a single displayed level, not an order-book update.

use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    str::FromStr,
};

pub const SCHEMA_VERSION: &str = "okx.observations.v1";

#[derive(Clone, Debug)]
pub struct TradeInput {
    pub instrument_id: String,
    pub channel: String,
    pub trade_id: String,
    pub price: String,
    pub base_quantity: String,
    pub side: String,
    pub event_at_ms: u64,
    pub available_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationTrade {
    pub schema_version: String,
    pub venue: String,
    pub environment: String,
    pub region: String,
    pub instrument_id: String,
    pub channel: String,
    pub trade_id: String,
    pub price: String,
    pub base_quantity: String,
    pub quote_notional: String,
    pub side: String,
    pub event_at_ms: u64,
    pub available_at_ms: u64,
    pub aggregate_count: u32,
    pub metadata_revision: String,
    pub source_epoch: String,
    pub connection_id: String,
    pub receive_sequence: u64,
    pub raw_sha256: String,
    pub quality: String,
    pub reason_codes: Vec<String>,
}

impl ObservationTrade {
    pub fn new(input: TradeInput) -> Result<Self> {
        let trade = Self {
            schema_version: SCHEMA_VERSION.into(),
            venue: "okx".into(),
            environment: "public".into(),
            region: "global".into(),
            instrument_id: input.instrument_id,
            channel: input.channel,
            trade_id: input.trade_id,
            price: input.price,
            base_quantity: input.base_quantity,
            quote_notional: String::new(),
            side: input.side,
            event_at_ms: input.event_at_ms,
            available_at_ms: input.available_at_ms,
            aggregate_count: 1,
            metadata_revision: "unqualified".into(),
            source_epoch: "unqualified".into(),
            connection_id: "unqualified".into(),
            receive_sequence: 0,
            raw_sha256: String::new(),
            quality: "unqualified".into(),
            reason_codes: vec!["MISSING_CAPTURE_LINEAGE".into()],
        };
        trade.with_calculated_notional()
    }

    fn with_calculated_notional(mut self) -> Result<Self> {
        self.quote_notional =
            exact_decimal_product(self.price_decimal()?, self.quantity_decimal()?)?
                .normalize()
                .to_string();
        self.validate()?;
        Ok(self)
    }

    pub fn quote_notional(&self) -> Result<Decimal> {
        exact_decimal_product(self.price_decimal()?, self.quantity_decimal()?)
    }

    pub fn price_decimal(&self) -> Result<Decimal> {
        validate_positive_decimal(&self.price, "price")
    }

    pub fn quantity_decimal(&self) -> Result<Decimal> {
        validate_positive_decimal(&self.base_quantity, "base quantity")
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SCHEMA_VERSION && self.venue == "okx",
            "unsupported observation schema or venue"
        );
        ensure!(
            matches!(self.environment.as_str(), "public" | "demo"),
            "invalid environment"
        );
        ensure!(
            matches!(self.region.as_str(), "global" | "eea"),
            "invalid region"
        );
        ensure!(
            valid_spot_instrument(&self.instrument_id),
            "invalid spot instrument identity"
        );
        ensure!(
            matches!(
                self.channel.as_str(),
                "trades" | "trades-all" | "rest-trades"
            ),
            "unsupported trade channel"
        );
        ensure!(
            !self.trade_id.is_empty()
                && self.trade_id.len() <= 30
                && self.trade_id.bytes().all(|b| b.is_ascii_digit()),
            "trade identity must be a raw decimal ID"
        );
        self.price_decimal()?;
        self.quantity_decimal()?;
        let expected_notional =
            exact_decimal_product(self.price_decimal()?, self.quantity_decimal()?)?
                .normalize()
                .to_string();
        ensure!(
            self.quote_notional == expected_notional,
            "quote notional does not match exact price times size"
        );
        ensure!(
            matches!(self.side.as_str(), "buy" | "sell" | "unknown"),
            "invalid trade side"
        );
        ensure!(self.event_at_ms > 0, "invalid event timestamp");
        ensure!(self.available_at_ms > 0, "invalid trade availability time");
        ensure!(
            (1..=1_000_000).contains(&self.aggregate_count)
                && (self.channel == "trades" || self.aggregate_count == 1),
            "invalid aggregate trade count for channel"
        );
        Ok(())
    }

    fn fingerprint(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            self.event_at_ms, self.price, self.base_quantity, self.side, self.aggregate_count
        )
    }
}

pub fn parse_trade_payload(
    payload: &Value,
    instrument_id: &str,
    channel: &str,
    available_at_ms: u64,
) -> Result<ObservationTrade> {
    ensure!(
        valid_spot_instrument(instrument_id),
        "invalid spot instrument identity"
    );
    let expected_inst = instrument_id
        .rsplit(':')
        .next()
        .context("missing instrument")?;
    ensure!(
        payload["instId"].as_str() == Some(expected_inst),
        "trade instrument scope mismatch"
    );
    let raw_id = payload["tradeId"]
        .as_str()
        .context("tradeId must be a string")?;
    let ts = payload["ts"]
        .as_str()
        .context("trade timestamp must be a string")?;
    let event_at_ms = parse_u64(ts, "trade timestamp")?;
    let count = match payload.get("count") {
        None | Some(Value::Null) => 1,
        Some(value) => parse_u64(value.as_str().context("count must be a string")?, "count")?
            .try_into()
            .context("aggregate count exceeds u32")?,
    };
    let side = match payload.get("side").and_then(Value::as_str) {
        Some("buy") => "buy",
        Some("sell") => "sell",
        None | Some("") | Some("unknown") => "unknown",
        _ => bail!("unsupported trade side encoding"),
    };
    let trade = ObservationTrade {
        schema_version: SCHEMA_VERSION.into(),
        venue: "okx".into(),
        environment: "public".into(),
        region: "global".into(),
        instrument_id: instrument_id.to_string(),
        channel: channel.to_string(),
        trade_id: raw_id.to_string(),
        price: payload["px"]
            .as_str()
            .context("price must be a string")?
            .to_string(),
        base_quantity: payload["sz"]
            .as_str()
            .context("size must be a string")?
            .to_string(),
        side: side.to_string(),
        event_at_ms,
        available_at_ms,
        aggregate_count: count,
        quote_notional: String::new(),
        metadata_revision: "unqualified".into(),
        source_epoch: "unqualified".into(),
        connection_id: "unqualified".into(),
        receive_sequence: 0,
        raw_sha256: String::new(),
        quality: "unqualified".into(),
        reason_codes: vec!["MISSING_CAPTURE_LINEAGE".into()],
    };
    trade.with_calculated_notional()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BboSnapshot {
    pub schema_version: String,
    pub venue: String,
    pub environment: String,
    pub region: String,
    pub instrument_id: String,
    pub channel: String,
    pub bid_price: String,
    pub bid_quantity: String,
    pub ask_price: String,
    pub ask_quantity: String,
    pub sequence: Option<String>,
    pub event_at_ms: u64,
    pub available_at_ms: u64,
    pub metadata_revision: String,
    pub source_epoch: String,
    pub snapshot: bool,
    pub depth_scope: String,
    pub quality: String,
    pub reason_codes: Vec<String>,
}

impl BboSnapshot {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == SCHEMA_VERSION && self.venue == "okx",
            "unsupported BBO schema or venue"
        );
        ensure!(
            matches!(self.environment.as_str(), "public" | "demo"),
            "invalid BBO environment"
        );
        ensure!(
            matches!(self.region.as_str(), "global" | "eea"),
            "invalid BBO region"
        );
        ensure!(
            valid_spot_instrument(&self.instrument_id),
            "invalid spot instrument identity"
        );
        ensure!(self.channel == "bbo-tbt", "BBO requires bbo-tbt channel");
        ensure!(
            self.snapshot && self.depth_scope == "single_level",
            "BBO must remain a single-level snapshot"
        );
        parse_decimal(&self.bid_price, "bid price")?;
        parse_decimal(&self.bid_quantity, "bid quantity")?;
        parse_decimal(&self.ask_price, "ask price")?;
        parse_decimal(&self.ask_quantity, "ask quantity")?;
        ensure!(
            self.event_at_ms > 0 && self.available_at_ms > 0,
            "invalid BBO timestamp"
        );
        ensure!(
            matches!(
                self.quality.as_str(),
                "raw" | "unqualified" | "qualified" | "invalid"
            ),
            "invalid BBO quality"
        );
        Ok(())
    }

    /// A crossed or locked snapshot remains available as raw evidence but has no executable spread.
    pub fn spread_bps(&self) -> Result<Option<Decimal>> {
        self.validate()?;
        let bid = parse_decimal(&self.bid_price, "bid price")?;
        let ask = parse_decimal(&self.ask_price, "ask price")?;
        let bid_size = parse_decimal(&self.bid_quantity, "bid quantity")?;
        let ask_size = parse_decimal(&self.ask_quantity, "ask quantity")?;
        if ask <= bid
            || bid <= Decimal::ZERO
            || ask <= Decimal::ZERO
            || bid_size <= Decimal::ZERO
            || ask_size <= Decimal::ZERO
            || self.available_at_ms < self.event_at_ms
            || self.quality != "qualified"
            || !self.reason_codes.is_empty()
        {
            return Ok(None);
        }
        let mid = (ask + bid) / Decimal::TWO;
        Ok(Some(Decimal::from(10_000) * (ask - bid) / mid))
    }
}

pub fn parse_bbo_payload(
    payload: &Value,
    instrument_id: &str,
    region: &str,
    metadata_revision: &str,
    source_epoch: &str,
    receive_at_ms: u64,
) -> Result<BboSnapshot> {
    ensure!(
        valid_spot_instrument(instrument_id),
        "invalid spot instrument identity"
    );
    let expected_inst = instrument_id
        .rsplit(':')
        .next()
        .context("missing instrument")?;
    if let Some(payload_inst) = payload.get("instId") {
        ensure!(
            payload_inst.as_str() == Some(expected_inst),
            "BBO instrument scope mismatch"
        );
    }
    let event_at_ms = parse_u64(
        payload["ts"]
            .as_str()
            .context("BBO timestamp must be a string")?,
        "BBO timestamp",
    )?;
    let mut reason_codes = Vec::new();
    if receive_at_ms < event_at_ms {
        reason_codes.push("FUTURE_EVENT_CLOCK".to_string());
    }
    let (bid_px, bid_sz, _) = parse_bbo_level(payload.get("bids"), "bids")?;
    let (ask_px, ask_sz, _) = parse_bbo_level(payload.get("asks"), "asks")?;
    let bid = parse_decimal(bid_px, "bid price")?;
    let ask = parse_decimal(ask_px, "ask price")?;
    let bid_size = parse_decimal(bid_sz, "bid quantity")?;
    let ask_size = parse_decimal(ask_sz, "ask quantity")?;
    if bid <= Decimal::ZERO
        || ask <= Decimal::ZERO
        || bid_size <= Decimal::ZERO
        || ask_size <= Decimal::ZERO
        || ask <= bid
    {
        reason_codes.push("INVALID_BBO_STATE".to_string());
    }
    let snapshot = BboSnapshot {
        schema_version: SCHEMA_VERSION.into(),
        venue: "okx".into(),
        environment: "public".into(),
        region: region.into(),
        instrument_id: instrument_id.into(),
        channel: "bbo-tbt".into(),
        bid_price: bid_px.into(),
        bid_quantity: bid_sz.into(),
        ask_price: ask_px.into(),
        ask_quantity: ask_sz.into(),
        sequence: Some(
            payload
                .get("seqId")
                .and_then(Value::as_u64)
                .context("BBO seqId must be a nonnegative integer")?
                .to_string(),
        ),
        event_at_ms,
        available_at_ms: receive_at_ms,
        metadata_revision: metadata_revision.into(),
        source_epoch: source_epoch.into(),
        snapshot: true,
        depth_scope: "single_level".into(),
        quality: if reason_codes.is_empty() {
            "raw"
        } else {
            "unqualified"
        }
        .into(),
        reason_codes,
    };
    snapshot.validate()?;
    Ok(snapshot)
}

fn parse_bbo_level<'a>(value: Option<&'a Value>, field: &str) -> Result<(&'a str, &'a str, u64)> {
    let levels = value
        .and_then(Value::as_array)
        .with_context(|| format!("BBO {field} must be a level array"))?;
    ensure!(
        levels.len() == 1,
        "BBO {field} must contain exactly one displayed level"
    );
    let tuple = levels[0]
        .as_array()
        .with_context(|| format!("BBO {field} level must be a tuple"))?;
    ensure!(
        tuple.len() == 4,
        "BBO {field} level tuple must have four fields"
    );
    let price = tuple[0]
        .as_str()
        .with_context(|| format!("BBO {field} price must be a string"))?;
    let size = tuple[1]
        .as_str()
        .with_context(|| format!("BBO {field} size must be a string"))?;
    ensure!(
        tuple[2].as_str() == Some("0"),
        "BBO level action field must be zero"
    );
    let order_count = tuple[3]
        .as_u64()
        .or_else(|| tuple[3].as_str().and_then(|raw| raw.parse::<u64>().ok()))
        .context("BBO level order count must be a nonnegative integer")?;
    Ok((price, size, order_count))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DedupDecision {
    New,
    Duplicate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DedupCheckpoint {
    pub schema_version: String,
    pub limit: usize,
    pub entries: Vec<DedupCheckpointEntry>,
    pub overlap_verified: bool,
    pub retention_exhausted: bool,
    pub unverified_instruments: Vec<String>,
    pub retention_exhausted_instruments: Vec<String>,
    pub evicted_event_time_floors: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DedupCheckpointEntry {
    pub identity_sha256: String,
    pub instrument_id: String,
    pub event_at_ms: u64,
    pub fingerprint: String,
}

/// Bounded overlap state. Eviction permanently removes the ability to claim full overlap coverage.
pub struct TradeDeduplicator {
    limit: usize,
    identities: BTreeMap<String, String>,
    identity_instruments: BTreeMap<String, String>,
    identity_event_times: BTreeMap<String, u64>,
    insertion_order: VecDeque<String>,
    overlap_verified: bool,
    retention_exhausted: bool,
    unverified_instruments: BTreeSet<String>,
    retention_exhausted_instruments: BTreeSet<String>,
    evicted_event_time_floors: BTreeMap<String, u64>,
}

impl TradeDeduplicator {
    pub fn new(limit: usize) -> Result<Self> {
        ensure!(
            (1..=8_192).contains(&limit),
            "dedup limit must be 1..8192 so checkpoints stay within the shared row bound"
        );
        Ok(Self {
            limit,
            identities: BTreeMap::new(),
            identity_instruments: BTreeMap::new(),
            identity_event_times: BTreeMap::new(),
            insertion_order: VecDeque::new(),
            overlap_verified: true,
            retention_exhausted: false,
            unverified_instruments: BTreeSet::new(),
            retention_exhausted_instruments: BTreeSet::new(),
            evicted_event_time_floors: BTreeMap::new(),
        })
    }

    pub fn begin_epoch(&mut self) {
        self.overlap_verified = false;
        self.unverified_instruments
            .extend(self.identity_instruments.values().cloned());
    }

    pub fn begin_epoch_for<'a>(&mut self, instruments: impl IntoIterator<Item = &'a str>) {
        self.unverified_instruments
            .extend(instruments.into_iter().map(str::to_owned));
        self.overlap_verified = self.unverified_instruments.is_empty();
    }

    pub fn admit(&mut self, trade: ObservationTrade) -> Result<DedupDecision> {
        trade.validate()?;
        let key = trade_identity_sha256(&trade)?;
        let fingerprint = trade.fingerprint();
        if let Some(previous) = self.identities.get(&key) {
            ensure!(
                previous == &fingerprint,
                "conflicting duplicate trade invalidates observation epoch"
            );
            if !self
                .retention_exhausted_instruments
                .contains(&trade.instrument_id)
            {
                self.unverified_instruments.remove(&trade.instrument_id);
            }
            self.overlap_verified = self.unverified_instruments.is_empty();
            return Ok(DedupDecision::Duplicate);
        }
        if self.identities.len() == self.limit
            && let Some(oldest) = self.insertion_order.pop_front()
        {
            self.identities.remove(&oldest);
            if let Some(instrument_id) = self.identity_instruments.remove(&oldest) {
                self.unverified_instruments.insert(instrument_id.clone());
                self.retention_exhausted_instruments
                    .insert(instrument_id.clone());
                if let Some(event_at_ms) = self.identity_event_times.remove(&oldest) {
                    let floor = self
                        .evicted_event_time_floors
                        .entry(instrument_id)
                        .or_default();
                    *floor = (*floor).max(event_at_ms);
                }
            }
            self.overlap_verified = false;
            self.retention_exhausted = true;
        }
        self.insertion_order.push_back(key.clone());
        self.identities.insert(key.clone(), fingerprint);
        self.identity_instruments
            .insert(key.clone(), trade.instrument_id.clone());
        self.identity_event_times.insert(key, trade.event_at_ms);
        Ok(DedupDecision::New)
    }

    /// Checks classification without changing overlap or retained identity state.
    pub fn preview(&self, trade: &ObservationTrade) -> Result<DedupDecision> {
        trade.validate()?;
        let key = trade_identity_sha256(trade)?;
        if let Some(previous) = self.identities.get(&key) {
            ensure!(
                previous == &trade.fingerprint(),
                "conflicting duplicate trade invalidates observation epoch"
            );
            Ok(DedupDecision::Duplicate)
        } else {
            Ok(DedupDecision::New)
        }
    }

    pub fn overlap_verified(&self) -> bool {
        self.overlap_verified
    }
    pub fn overlap_verified_for(&self, instrument_id: &str) -> bool {
        !self.unverified_instruments.contains(instrument_id)
    }

    pub fn has_history_for(&self, instrument_id: &str) -> bool {
        self.identity_instruments
            .values()
            .any(|value| value == instrument_id)
            || self.retention_exhausted_instruments.contains(instrument_id)
            || self.unverified_instruments.contains(instrument_id)
            || self.evicted_event_time_floors.contains_key(instrument_id)
    }

    pub fn event_is_after_retention_floor(&self, instrument_id: &str, event_at_ms: u64) -> bool {
        self.evicted_event_time_floors
            .get(instrument_id)
            .is_none_or(|floor| event_at_ms > *floor)
    }

    pub fn checkpoint(&self) -> DedupCheckpoint {
        DedupCheckpoint {
            schema_version: "okx.observations.dedup-checkpoint.v2".into(),
            limit: self.limit,
            entries: self
                .insertion_order
                .iter()
                .filter_map(|identity_sha256| {
                    let fingerprint = self.identities.get(identity_sha256)?;
                    let instrument_id = self.identity_instruments.get(identity_sha256)?;
                    let event_at_ms = self.identity_event_times.get(identity_sha256)?;
                    Some(DedupCheckpointEntry {
                        identity_sha256: identity_sha256.clone(),
                        instrument_id: instrument_id.clone(),
                        event_at_ms: *event_at_ms,
                        fingerprint: fingerprint.clone(),
                    })
                })
                .collect(),
            overlap_verified: self.overlap_verified,
            retention_exhausted: self.retention_exhausted,
            unverified_instruments: self.unverified_instruments.iter().cloned().collect(),
            retention_exhausted_instruments: self
                .retention_exhausted_instruments
                .iter()
                .cloned()
                .collect(),
            evicted_event_time_floors: self.evicted_event_time_floors.clone(),
        }
    }

    pub fn restore(checkpoint: DedupCheckpoint) -> Result<Self> {
        ensure!(
            checkpoint.schema_version == "okx.observations.dedup-checkpoint.v2",
            "unsupported dedup checkpoint schema"
        );
        ensure!(
            (1..=8_192).contains(&checkpoint.limit),
            "dedup checkpoint limit invalid"
        );
        ensure!(
            checkpoint.entries.len() <= checkpoint.limit,
            "dedup checkpoint exceeds configured limit"
        );
        let mut state = Self::new(checkpoint.limit)?;
        for entry in checkpoint.entries {
            ensure!(
                valid_spot_instrument(&entry.instrument_id),
                "dedup checkpoint instrument invalid"
            );
            ensure!(
                entry.identity_sha256.len() == 64
                    && entry
                        .identity_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()),
                "dedup checkpoint identity hash invalid"
            );
            ensure!(
                !entry.fingerprint.is_empty()
                    && entry.fingerprint.len() <= 512
                    && !entry
                        .fingerprint
                        .bytes()
                        .any(|byte| byte.is_ascii_control()),
                "dedup checkpoint fingerprint invalid"
            );
            ensure!(
                !state.identities.contains_key(&entry.identity_sha256),
                "duplicate key in dedup checkpoint"
            );
            state
                .insertion_order
                .push_back(entry.identity_sha256.clone());
            state
                .identity_instruments
                .insert(entry.identity_sha256.clone(), entry.instrument_id);
            state
                .identity_event_times
                .insert(entry.identity_sha256.clone(), entry.event_at_ms);
            state
                .identities
                .insert(entry.identity_sha256, entry.fingerprint);
        }
        state.overlap_verified = checkpoint.overlap_verified;
        state.retention_exhausted = checkpoint.retention_exhausted;
        state.unverified_instruments = checkpoint.unverified_instruments.into_iter().collect();
        state.retention_exhausted_instruments = checkpoint
            .retention_exhausted_instruments
            .into_iter()
            .collect();
        state.evicted_event_time_floors = checkpoint.evicted_event_time_floors;
        Ok(state)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedNotionalBar {
    pub schema_version: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub available_at_ms: u64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub quote_notional: Decimal,
    pub overshoot: Decimal,
    pub trade_ids: Vec<String>,
    pub trade_id_count: u64,
    pub trade_ids_truncated: bool,
    pub trade_ids_sha256: String,
    pub first_trade_id: String,
    pub last_trade_id: String,
    pub close_reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenNotionalBar {
    start_ms: u64,
    end_ms: u64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    quote_notional: Decimal,
    trade_ids: Vec<String>,
    trade_id_count: u64,
    trade_ids_truncated: bool,
    trade_ids_sha256: String,
    first_trade_id: Option<String>,
    last_trade_id: Option<String>,
    available_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotionalBarBuilder {
    threshold: Decimal,
    open: Option<OpenNotionalBar>,
}

impl NotionalBarBuilder {
    pub fn new(threshold: Decimal) -> Result<Self> {
        ensure!(
            threshold > Decimal::ZERO,
            "notional threshold must be positive"
        );
        Ok(Self {
            threshold,
            open: None,
        })
    }

    pub fn threshold(&self) -> Decimal {
        self.threshold
    }

    pub fn reset_open(&mut self) -> bool {
        self.open.take().is_some()
    }

    pub fn has_open(&self) -> bool {
        self.open.is_some()
    }

    pub fn push(&mut self, trade: ObservationTrade) -> Result<Vec<CompletedNotionalBar>> {
        trade.validate()?;
        let price = trade.price_decimal()?;
        let notional = trade.quote_notional()?;
        let bar = self.open.get_or_insert_with(|| OpenNotionalBar {
            start_ms: trade.event_at_ms,
            end_ms: trade.event_at_ms,
            open: price,
            high: price,
            low: price,
            close: price,
            quote_notional: Decimal::ZERO,
            trade_ids: Vec::new(),
            trade_id_count: 0,
            trade_ids_truncated: false,
            trade_ids_sha256: "0".repeat(64),
            first_trade_id: None,
            last_trade_id: None,
            available_at_ms: trade.available_at_ms,
        });
        bar.end_ms = trade.event_at_ms;
        bar.available_at_ms = trade.available_at_ms;
        bar.high = bar.high.max(price);
        bar.low = bar.low.min(price);
        bar.close = price;
        bar.quote_notional = exact_decimal_add(bar.quote_notional, notional)?;
        bar.trade_id_count = bar
            .trade_id_count
            .checked_add(1)
            .context("notional bar trade count overflow")?;
        bar.first_trade_id
            .get_or_insert_with(|| trade.trade_id.clone());
        bar.trade_ids_sha256 =
            payload_sha256(format!("{}:{}", bar.trade_ids_sha256, trade.trade_id).as_bytes());
        bar.last_trade_id = Some(trade.trade_id.clone());
        if bar.trade_ids.len() < 16 {
            bar.trade_ids.push(trade.trade_id);
        } else {
            bar.trade_ids_truncated = true;
        }
        if bar.quote_notional < self.threshold {
            return Ok(Vec::new());
        }
        let bar = self.open.take().expect("bar exists after push");
        Ok(vec![CompletedNotionalBar {
            schema_version: "okx.observations.notional-bar.v1".into(),
            start_ms: bar.start_ms,
            end_ms: bar.end_ms,
            available_at_ms: bar.available_at_ms,
            open: bar.open.normalize().to_string(),
            high: bar.high.normalize().to_string(),
            low: bar.low.normalize().to_string(),
            close: bar.close.normalize().to_string(),
            overshoot: exact_decimal_sub(bar.quote_notional, self.threshold)?,
            quote_notional: bar.quote_notional,
            trade_ids: bar.trade_ids,
            trade_id_count: bar.trade_id_count,
            trade_ids_truncated: bar.trade_ids_truncated,
            trade_ids_sha256: bar.trade_ids_sha256,
            first_trade_id: bar
                .first_trade_id
                .context("notional bar has no first trade ID")?,
            last_trade_id: bar
                .last_trade_id
                .context("notional bar has no last trade ID")?,
            close_reason: "THRESHOLD".into(),
        }])
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedTimeBar {
    pub schema_version: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub available_at_ms: u64,
    pub open: Option<String>,
    pub high: Option<String>,
    pub low: Option<String>,
    pub close: Option<String>,
    pub base_quantity: Decimal,
    pub quote_notional: Decimal,
    pub trade_count: usize,
    pub quality: String,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimedBucket {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    base_quantity: Decimal,
    quote_notional: Decimal,
    trade_count: usize,
}

/// Event-time, half-open fixed bars. `finalize_through` is called only by an explicit recorded
/// timer/watermark; the timer availability becomes the bar availability.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixedTimeBars {
    duration_ms: u64,
    buckets: BTreeMap<u64, TimedBucket>,
    last_finalized_end_ms: u64,
    #[serde(default)]
    unavailable_intervals: BTreeMap<u64, BTreeSet<String>>,
}

impl FixedTimeBars {
    pub fn new(duration_ms: u64) -> Result<Self> {
        Self::new_at(duration_ms, 0)
    }

    pub fn new_at(duration_ms: u64, start_at_ms: u64) -> Result<Self> {
        ensure!(
            duration_ms > 0 && duration_ms <= 86_400_000,
            "invalid bar duration"
        );
        Ok(Self {
            duration_ms,
            buckets: BTreeMap::new(),
            last_finalized_end_ms: start_at_ms / duration_ms * duration_ms,
            unavailable_intervals: BTreeMap::new(),
        })
    }

    pub fn push(&mut self, trade: ObservationTrade) -> Result<bool> {
        trade.validate()?;
        let start = trade.event_at_ms / self.duration_ms * self.duration_ms;
        let end = start
            .checked_add(self.duration_ms)
            .context("time bar boundary overflow")?;
        if end <= self.last_finalized_end_ms {
            return Ok(false);
        }
        let price = trade.price_decimal()?;
        let bucket = self.buckets.entry(start).or_insert(TimedBucket {
            open: price,
            high: price,
            low: price,
            close: price,
            base_quantity: Decimal::ZERO,
            quote_notional: Decimal::ZERO,
            trade_count: 0,
        });
        bucket.high = bucket.high.max(price);
        bucket.low = bucket.low.min(price);
        bucket.close = price;
        bucket.base_quantity = exact_decimal_add(bucket.base_quantity, trade.quantity_decimal()?)?;
        bucket.quote_notional = exact_decimal_add(bucket.quote_notional, trade.quote_notional()?)?;
        bucket.trade_count += 1;
        Ok(true)
    }

    pub fn finalize_through(
        &mut self,
        timer_at_ms: u64,
        coverage_qualified: bool,
    ) -> Result<Vec<CompletedTimeBar>> {
        let final_end = timer_at_ms / self.duration_ms * self.duration_ms;
        ensure!(
            final_end >= self.last_finalized_end_ms,
            "time bar timer moved backward; source epoch reset is required"
        );
        let pending_intervals =
            final_end.saturating_sub(self.last_finalized_end_ms) / self.duration_ms;
        ensure!(
            pending_intervals <= 2,
            "time bar timer discontinuity exceeds bounded catch-up; source gap recovery is required"
        );
        let mut out = Vec::new();
        let mut start = self.last_finalized_end_ms;
        while start < final_end {
            let end = start
                .checked_add(self.duration_ms)
                .context("time bar boundary overflow")?;
            let bucket = self.buckets.remove(&start);
            let mut reasons = self
                .unavailable_intervals
                .remove(&start)
                .unwrap_or_default();
            if !coverage_qualified {
                reasons.insert("SOURCE_COVERAGE_UNVERIFIED".into());
            }
            if !reasons.is_empty() {
                out.push(CompletedTimeBar {
                    schema_version: "okx.observations.time-bar.v1".into(),
                    start_ms: start,
                    end_ms: end,
                    available_at_ms: timer_at_ms,
                    open: None,
                    high: None,
                    low: None,
                    close: None,
                    base_quantity: Decimal::ZERO,
                    quote_notional: Decimal::ZERO,
                    trade_count: 0,
                    quality: "unavailable".into(),
                    reason_codes: reasons.into_iter().collect(),
                });
            } else if let Some(bucket) = bucket {
                out.push(CompletedTimeBar {
                    schema_version: "okx.observations.time-bar.v1".into(),
                    start_ms: start,
                    end_ms: end,
                    available_at_ms: timer_at_ms,
                    open: Some(bucket.open.normalize().to_string()),
                    high: Some(bucket.high.normalize().to_string()),
                    low: Some(bucket.low.normalize().to_string()),
                    close: Some(bucket.close.normalize().to_string()),
                    base_quantity: bucket.base_quantity,
                    quote_notional: bucket.quote_notional,
                    trade_count: bucket.trade_count,
                    quality: "qualified".into(),
                    reason_codes: Vec::new(),
                });
            } else if coverage_qualified {
                out.push(CompletedTimeBar {
                    schema_version: "okx.observations.time-bar.v1".into(),
                    start_ms: start,
                    end_ms: end,
                    available_at_ms: timer_at_ms,
                    open: None,
                    high: None,
                    low: None,
                    close: None,
                    base_quantity: Decimal::ZERO,
                    quote_notional: Decimal::ZERO,
                    trade_count: 0,
                    quality: "qualified_empty".into(),
                    reason_codes: Vec::new(),
                });
            }
            start = end;
        }
        self.last_finalized_end_ms = final_end;
        Ok(out)
    }

    pub fn reset_at(&mut self, timestamp_ms: u64) {
        self.buckets.clear();
        self.unavailable_intervals.clear();
        self.last_finalized_end_ms = timestamp_ms / self.duration_ms * self.duration_ms;
    }

    pub fn last_finalized_end_ms(&self) -> u64 {
        self.last_finalized_end_ms
    }

    pub fn requires_epoch_reset(&self, timer_at_ms: u64) -> bool {
        let final_end = timer_at_ms / self.duration_ms * self.duration_ms;
        final_end < self.last_finalized_end_ms
            || final_end.saturating_sub(self.last_finalized_end_ms) / self.duration_ms > 2
    }

    pub fn invalidate_event_time(&mut self, event_at_ms: u64, reason: &str) {
        let start = event_at_ms / self.duration_ms * self.duration_ms;
        let maximum_pending_start = self
            .last_finalized_end_ms
            .saturating_add(self.duration_ms.saturating_mul(2));
        // Late and implausibly future events remain in the journal as evidence, but cannot
        // create unbounded checkpoint state outside the current bounded feature horizon.
        if start < self.last_finalized_end_ms || start > maximum_pending_start {
            return;
        }
        self.unavailable_intervals
            .entry(start)
            .or_default()
            .insert(reason.to_owned());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayCheckpoint {
    pub schema_version: String,
    pub channel: String,
    pub cursor: crate::observation_journal::JournalCursor,
    pub bar_duration_ms: u64,
    pub configuration_identity: String,
    pub dedup: DedupCheckpoint,
    pub time_bars: BTreeMap<String, FixedTimeBars>,
    pub notional_bars: BTreeMap<String, NotionalBarBuilder>,
}

#[derive(Default)]
pub struct ReplayOutput {
    pub time_bars: Vec<(String, CompletedTimeBar)>,
    pub notional_bars: Vec<(String, CompletedNotionalBar)>,
}

#[derive(Default)]
pub struct ReplayExecution {
    pub records_scanned: usize,
    pub last_cursor: Option<crate::observation_journal::JournalCursor>,
    pub outputs: Vec<(crate::observation_journal::JournalCursor, ReplayOutput)>,
}

/// Replays normalized journal payloads through the same deduplication and bar transitions used by capture.
pub struct ObservationReplay {
    channel: String,
    configuration_identity: String,
    bar_duration_ms: u64,
    dedup: TradeDeduplicator,
    time_bars: BTreeMap<String, FixedTimeBars>,
    notional_bars: BTreeMap<String, NotionalBarBuilder>,
}

impl ObservationReplay {
    pub fn new(
        channel: impl Into<String>,
        configuration_identity: impl Into<String>,
        instruments: &BTreeSet<String>,
        bar_duration_ms: u64,
        dedup_limit: usize,
        notional_thresholds: BTreeMap<String, Decimal>,
        start_at_ms: u64,
    ) -> Result<Self> {
        ensure!(
            bar_duration_ms > 0 && bar_duration_ms <= 86_400_000,
            "invalid replay bar duration"
        );
        let mut time_bars = BTreeMap::new();
        let mut notional_bars = BTreeMap::new();
        for instrument in instruments {
            ensure!(
                valid_spot_instrument(instrument),
                "invalid replay instrument identity"
            );
            time_bars.insert(
                instrument.to_owned(),
                FixedTimeBars::new_at(bar_duration_ms, start_at_ms)?,
            );
        }
        for (instrument, threshold) in notional_thresholds {
            ensure!(
                instruments.contains(&instrument),
                "notional replay instrument is not configured"
            );
            notional_bars.insert(instrument, NotionalBarBuilder::new(threshold)?);
        }
        Ok(Self {
            channel: channel.into(),
            configuration_identity: configuration_identity.into(),
            bar_duration_ms,
            dedup: TradeDeduplicator::new(dedup_limit)?,
            time_bars,
            notional_bars,
        })
    }

    pub fn restore(checkpoint: ReplayCheckpoint, restart_live_epoch: bool) -> Result<Self> {
        ensure!(
            checkpoint.schema_version == "okx.observations.replay-checkpoint.v1",
            "unsupported replay checkpoint schema"
        );
        ensure!(
            checkpoint.bar_duration_ms > 0 && checkpoint.bar_duration_ms <= 86_400_000,
            "invalid checkpoint bar duration"
        );
        ensure!(
            checkpoint
                .time_bars
                .values()
                .all(|bars| bars.duration_ms == checkpoint.bar_duration_ms),
            "checkpoint bar duration mismatch"
        );
        ensure!(
            checkpoint
                .time_bars
                .keys()
                .all(|key| valid_spot_instrument(key)),
            "invalid checkpoint time bar instrument"
        );
        ensure!(
            checkpoint
                .notional_bars
                .keys()
                .all(|key| valid_spot_instrument(key)),
            "invalid checkpoint notional bar instrument"
        );
        let mut dedup = TradeDeduplicator::restore(checkpoint.dedup)?;
        if restart_live_epoch {
            dedup.begin_epoch();
        }
        Ok(Self {
            channel: checkpoint.channel,
            configuration_identity: checkpoint.configuration_identity,
            bar_duration_ms: checkpoint.bar_duration_ms,
            dedup,
            time_bars: checkpoint.time_bars,
            notional_bars: checkpoint.notional_bars,
        })
    }

    pub fn checkpoint(
        &self,
        channel: impl Into<String>,
        cursor: crate::observation_journal::JournalCursor,
    ) -> ReplayCheckpoint {
        ReplayCheckpoint {
            schema_version: "okx.observations.replay-checkpoint.v1".into(),
            channel: channel.into(),
            cursor,
            bar_duration_ms: self.bar_duration_ms,
            configuration_identity: self.configuration_identity.clone(),
            dedup: self.dedup.checkpoint(),
            time_bars: self.time_bars.clone(),
            notional_bars: self.notional_bars.clone(),
        }
    }

    pub fn validate_notional_thresholds(&self, expected: &BTreeMap<String, Decimal>) -> Result<()> {
        let actual = self
            .notional_bars
            .iter()
            .map(|(instrument, builder)| (instrument.clone(), builder.threshold()))
            .collect::<BTreeMap<_, _>>();
        ensure!(
            actual == *expected,
            "configured notional threshold/calibration differs from durable checkpoint"
        );
        Ok(())
    }

    pub fn validate_configuration_identity(&self, expected: &str) -> Result<()> {
        ensure!(
            self.configuration_identity == expected,
            "observation metadata, calibration, profile, or channel identity differs from durable checkpoint"
        );
        Ok(())
    }

    pub fn has_open_notional(&self, instrument_id: &str) -> bool {
        self.notional_bars
            .get(instrument_id)
            .is_some_and(NotionalBarBuilder::has_open)
    }

    pub fn apply(&mut self, payload: &Value) -> Result<ReplayOutput> {
        let mut output = ReplayOutput::default();
        if payload
            .get("channel")
            .and_then(Value::as_str)
            .is_some_and(|channel| channel != self.channel)
        {
            return Ok(output);
        }
        if payload.get("record_type").and_then(Value::as_str) == Some("trade") {
            let observation: ObservationTrade =
                serde_json::from_value(payload["observation"].clone())
                    .context("invalid normalized trade journal observation")?;
            if self.dedup.admit(observation.clone())? == DedupDecision::New {
                if observation.quality == "qualified" {
                    if let Some(bars) = self.time_bars.get_mut(&observation.instrument_id) {
                        bars.push(observation.clone())?;
                    }
                    if let Some(builder) = self.notional_bars.get_mut(&observation.instrument_id) {
                        output.notional_bars.extend(
                            builder
                                .push(observation.clone())?
                                .into_iter()
                                .map(|bar| (observation.instrument_id.clone(), bar)),
                        );
                    }
                } else if let Some(bars) = self.time_bars.get_mut(&observation.instrument_id) {
                    bars.invalidate_event_time(observation.event_at_ms, "UNQUALIFIED_TRADE");
                }
            }
        } else if payload.get("event").and_then(Value::as_str) == Some("gap") {
            let instrument = payload
                .get("instrument_id")
                .and_then(Value::as_str)
                .context("gap record has no canonical instrument")?;
            let at_ms = payload
                .get("available_at_ms")
                .and_then(Value::as_u64)
                .context("gap record has no availability time")?;
            self.dedup.begin_epoch_for([instrument]);
            if let Some(bars) = self.time_bars.get_mut(instrument) {
                bars.reset_at(at_ms);
            }
            if let Some(builder) = self.notional_bars.get_mut(instrument) {
                builder.reset_open();
            }
        } else if payload.get("event").and_then(Value::as_str) == Some("timer")
            && payload.get("timer_kind").and_then(Value::as_str) == Some("fixed_time_bar")
        {
            let timer_at_ms = payload["available_at_ms"]
                .as_u64()
                .context("timer has no availability time")?;
            for (instrument, bars) in &mut self.time_bars {
                let coverage_qualified = payload
                    .get("coverage_by_instrument")
                    .and_then(Value::as_object)
                    .and_then(|coverage| coverage.get(instrument))
                    .and_then(|coverage| coverage.get("qualified"))
                    .and_then(Value::as_bool)
                    .context("fixed-time timer has no pair-scoped coverage qualification")?;
                output.time_bars.extend(
                    bars.finalize_through(timer_at_ms, coverage_qualified)?
                        .into_iter()
                        .map(|bar| (instrument.clone(), bar)),
                );
            }
        }
        Ok(output)
    }

    pub fn begin_live_epoch(&mut self) {
        self.dedup.begin_epoch();
    }

    pub fn replay_verified_journal(
        &mut self,
        root: impl AsRef<std::path::Path>,
        after: Option<&crate::observation_journal::JournalCursor>,
        max_records: usize,
    ) -> Result<ReplayExecution> {
        ensure!(max_records > 0, "replay record limit must be positive");
        let mut execution = ReplayExecution::default();
        let mut cursor_seen = after.is_none();
        crate::observation_journal::visit_verified_records(root, |entry| {
            if let Some(cursor) = after
                && !cursor_seen
            {
                if &entry.cursor == cursor {
                    cursor_seen = true;
                }
                return Ok(());
            }
            if entry.payload.get("channel").and_then(Value::as_str) != Some(self.channel.as_str()) {
                return Ok(());
            }
            execution.records_scanned = execution.records_scanned.saturating_add(1);
            ensure!(
                execution.records_scanned <= max_records,
                "verified observation replay suffix exceeds max_replay_records"
            );
            let output = self.apply(&entry.payload)?;
            execution.last_cursor = Some(entry.cursor.clone());
            if !output.time_bars.is_empty() || !output.notional_bars.is_empty() {
                execution.outputs.push((entry.cursor, output));
            }
            Ok(())
        })?;
        ensure!(
            cursor_seen,
            "replay checkpoint cursor is absent from the verified journal"
        );
        Ok(execution)
    }

    pub fn latest_checkpoint(
        root: impl AsRef<std::path::Path>,
        channel: &str,
    ) -> Result<Option<ReplayCheckpoint>> {
        let mut previous: Option<crate::observation_journal::JournalCursor> = None;
        let mut latest = None;
        crate::observation_journal::visit_verified_records(root, |entry| {
            if entry.payload.get("record_type").and_then(Value::as_str) == Some("checkpoint")
                && entry.payload.get("channel").and_then(Value::as_str) == Some(channel)
            {
                let checkpoint: ReplayCheckpoint =
                    serde_json::from_value(entry.payload["checkpoint"].clone())
                        .context("invalid replay checkpoint payload")?;
                ensure!(checkpoint.channel == channel, "checkpoint channel mismatch");
                ensure!(
                    previous.as_ref() == Some(&checkpoint.cursor),
                    "checkpoint cursor does not match the exact preceding durable record"
                );
                latest = Some(checkpoint);
            }
            previous = Some(entry.cursor);
            Ok(())
        })?;
        Ok(latest)
    }

    pub fn into_capture_parts(
        self,
    ) -> (
        TradeDeduplicator,
        BTreeMap<String, FixedTimeBars>,
        BTreeMap<String, NotionalBarBuilder>,
        String,
    ) {
        (
            self.dedup,
            self.time_bars,
            self.notional_bars,
            self.configuration_identity,
        )
    }
}

pub fn valid_spot_instrument(id: &str) -> bool {
    let Some((prefix, symbol)) = id.split_once(":spot:") else {
        return false;
    };
    prefix == "okx"
        && symbol.strip_suffix("-USDC").is_some_and(|base| {
            !base.is_empty()
                && base.len() <= 24
                && base
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        })
}

/// Perform exact nonnegative decimal arithmetic without `rust_decimal` rounding
/// when a product or aligned sum exceeds its internal 96-bit coefficient.
fn exact_decimal_product(left: Decimal, right: Decimal) -> Result<Decimal> {
    ensure!(
        left >= Decimal::ZERO && right >= Decimal::ZERO,
        "exact arithmetic requires nonnegative values"
    );
    let digits = multiply_decimal_digits(
        &left.mantissa().unsigned_abs().to_string(),
        &right.mantissa().unsigned_abs().to_string(),
    );
    decimal_from_digits(digits, left.scale() + right.scale())
}

fn exact_decimal_add(left: Decimal, right: Decimal) -> Result<Decimal> {
    ensure!(
        left >= Decimal::ZERO && right >= Decimal::ZERO,
        "exact arithmetic requires nonnegative values"
    );
    let scale = left.scale().max(right.scale());
    let left_digits = format!(
        "{}{}",
        left.mantissa().unsigned_abs(),
        "0".repeat((scale - left.scale()) as usize)
    );
    let right_digits = format!(
        "{}{}",
        right.mantissa().unsigned_abs(),
        "0".repeat((scale - right.scale()) as usize)
    );
    decimal_from_digits(add_decimal_digits(&left_digits, &right_digits), scale)
}

fn exact_decimal_sub(left: Decimal, right: Decimal) -> Result<Decimal> {
    ensure!(
        left >= right && right >= Decimal::ZERO,
        "exact decimal subtraction would be negative"
    );
    let scale = left.scale().max(right.scale());
    let left = format!(
        "{}{}",
        left.mantissa().unsigned_abs(),
        "0".repeat((scale - left.scale()) as usize)
    );
    let right = format!(
        "{}{}",
        right.mantissa().unsigned_abs(),
        "0".repeat((scale - right.scale()) as usize)
    );
    decimal_from_digits(subtract_decimal_digits(&left, &right), scale)
}

fn multiply_decimal_digits(left: &str, right: &str) -> String {
    let left = left
        .bytes()
        .rev()
        .map(|digit| (digit - b'0') as u32)
        .collect::<Vec<_>>();
    let right = right
        .bytes()
        .rev()
        .map(|digit| (digit - b'0') as u32)
        .collect::<Vec<_>>();
    let mut product = vec![0u32; left.len() + right.len()];
    for (left_index, left_digit) in left.iter().enumerate() {
        for (right_index, right_digit) in right.iter().enumerate() {
            product[left_index + right_index] += left_digit * right_digit;
        }
    }
    for index in 0..product.len() - 1 {
        let carry = product[index] / 10;
        product[index] %= 10;
        product[index + 1] += carry;
    }
    while product.len() > 1 && product.last() == Some(&0) {
        product.pop();
    }
    product
        .iter()
        .rev()
        .map(|digit| char::from(b'0' + *digit as u8))
        .collect()
}

fn add_decimal_digits(left: &str, right: &str) -> String {
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    let mut carry = 0u8;
    let mut sum = Vec::new();
    loop {
        let a = left.next().map(|byte| byte - b'0');
        let b = right.next().map(|byte| byte - b'0');
        if a.is_none() && b.is_none() && carry == 0 {
            break;
        }
        let value = a.unwrap_or(0) + b.unwrap_or(0) + carry;
        sum.push(b'0' + value % 10);
        carry = value / 10;
    }
    sum.reverse();
    String::from_utf8(sum).expect("ASCII decimal digits")
}

fn subtract_decimal_digits(left: &str, right: &str) -> String {
    debug_assert!(left.len() > right.len() || (left.len() == right.len() && left >= right));
    let left = left.bytes().rev();
    let mut right = right.bytes().rev();
    let mut borrow = 0i16;
    let mut difference = Vec::new();
    for a in left {
        let mut value = (a - b'0') as i16 - borrow;
        let b = right.next().map(|byte| (byte - b'0') as i16).unwrap_or(0);
        if value < b {
            value += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        difference.push(b'0' + (value - b) as u8);
    }
    while difference.len() > 1 && difference.last() == Some(&b'0') {
        difference.pop();
    }
    difference.reverse();
    String::from_utf8(difference).expect("ASCII decimal digits")
}

fn decimal_from_digits(mut digits: String, mut scale: u32) -> Result<Decimal> {
    while scale > 0 && digits.ends_with('0') {
        digits.pop();
        scale -= 1;
    }
    ensure!(scale <= 28, "exact decimal result exceeds supported scale");
    let significant = digits.trim_start_matches('0');
    let significant = if significant.is_empty() {
        "0"
    } else {
        significant
    };
    let maximum = "79228162514264337593543950335";
    ensure!(
        significant.len() < maximum.len()
            || (significant.len() == maximum.len() && significant <= maximum),
        "exact decimal result exceeds supported coefficient"
    );
    if scale > 0 {
        if digits.len() <= scale as usize {
            digits.insert_str(0, &"0".repeat(scale as usize + 1 - digits.len()));
        }
        digits.insert(digits.len() - scale as usize, '.');
    }
    Decimal::from_str(&digits).context("exact decimal result exceeds supported coefficient")
}

pub fn validate_positive_decimal(raw: &str, field: &str) -> Result<Decimal> {
    ensure!(
        !raw.is_empty() && raw.len() <= 64 && raw.bytes().all(|b| b.is_ascii_digit() || b == b'.'),
        "invalid {field} decimal syntax"
    );
    let value = Decimal::from_str_exact(raw).with_context(|| format!("invalid exact {field}"))?;
    ensure!(value > Decimal::ZERO, "{field} must be positive");
    Ok(value)
}

fn parse_decimal(raw: &str, field: &str) -> Result<Decimal> {
    ensure!(
        !raw.is_empty()
            && raw.len() <= 64
            && raw
                .bytes()
                .all(|b| b.is_ascii_digit() || b == b'.' || b == b'-'),
        "invalid {field} decimal syntax"
    );
    Decimal::from_str_exact(raw).with_context(|| format!("invalid exact {field}"))
}

pub fn validate_nonnegative_decimal(raw: &str, field: &str) -> Result<Decimal> {
    ensure!(
        !raw.is_empty() && raw.len() <= 64 && raw.bytes().all(|b| b.is_ascii_digit() || b == b'.'),
        "invalid {field} decimal syntax"
    );
    let value = Decimal::from_str_exact(raw).with_context(|| format!("invalid exact {field}"))?;
    ensure!(value >= Decimal::ZERO, "{field} must be nonnegative");
    Ok(value)
}

fn parse_u64(raw: &str, field: &str) -> Result<u64> {
    ensure!(
        !raw.is_empty() && raw.bytes().all(|b| b.is_ascii_digit()),
        "invalid {field}"
    );
    raw.parse().with_context(|| format!("{field} overflow"))
}

pub fn payload_sha256(raw: &[u8]) -> String {
    format!("{:x}", Sha256::digest(raw))
}

fn trade_identity_sha256(trade: &ObservationTrade) -> Result<String> {
    let identity = serde_json::to_vec(&(
        &trade.venue,
        &trade.environment,
        &trade.region,
        &trade.instrument_id,
        &trade.channel,
        &trade.trade_id,
    ))?;
    Ok(payload_sha256(&identity))
}

#[cfg(test)]
mod exact_arithmetic_tests {
    use super::*;

    #[test]
    fn product_with_unrepresentable_precision_fails_instead_of_rounding() {
        let value = Decimal::from_str_exact("1.0000000000000000000000000001").unwrap();
        assert!(exact_decimal_product(value, value).is_err());
    }

    #[test]
    fn aligned_sum_that_exceeds_decimal_coefficient_fails_instead_of_rounding() {
        let large = Decimal::from_str_exact("79228162514264337593543950335").unwrap();
        let tiny = Decimal::from_str_exact("0.0000000000000000000000000001").unwrap();
        assert!(exact_decimal_add(large, tiny).is_err());
    }

    #[test]
    fn exact_product_and_sum_preserve_representable_values() {
        let left = Decimal::from_str_exact("12.5").unwrap();
        let right = Decimal::from_str_exact("0.04").unwrap();
        assert_eq!(
            exact_decimal_product(left, right).unwrap().to_string(),
            "0.5"
        );
        assert_eq!(exact_decimal_add(left, right).unwrap().to_string(), "12.54");
    }
}
