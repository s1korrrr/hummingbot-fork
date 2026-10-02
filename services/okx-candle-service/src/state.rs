use crate::model::{Candle, now};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{Mutex, broadcast};

pub const BACKLOG: usize = 4096;
/// Snapshots share immutable candle allocations with current histories. The event
/// ring never retains thousands of fully expanded JSON copies of those histories.
#[derive(Clone, Debug)]
pub enum Event {
    Snapshot {
        metadata: Value,
        rows: Arc<[Arc<Candle>]>,
    },
    Other(Value),
}
impl Event {
    pub fn metadata(&self) -> &Value {
        match self {
            Self::Snapshot { metadata, .. } => metadata,
            Self::Other(value) => value,
        }
    }
    pub fn into_frame(self, history: usize) -> Value {
        match self {
            Self::Snapshot { mut metadata, rows } => {
                metadata["rows"] = json!(&rows[rows.len().saturating_sub(history)..]);
                metadata
            }
            Self::Other(value) => value,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PairState {
    pub rows: VecDeque<Arc<Candle>>,
    pub seed_base: Vec<Arc<Candle>>,
    pub live_head: Option<(u64, bool, f64)>,
    pub sequence: u64,
    pub ready: bool,
    pub online: bool,
    pub generation: u64,
    pub seeding: bool,
    pub received_at: f64,
    pub reason: String,
    /// Bit `n` is set while upstream lane `n` has an acknowledged subscription for this pair.
    /// A pair is online while any lane is up, so one socket reset never opens a gap.
    pub lanes: u8,
    /// Number of times an upstream lane serving this pair was lost.
    pub reconnects: u64,
    /// Rows or frames refused or ignored for this pair, keyed by a stable reason label.
    pub rejected: BTreeMap<&'static str, u64>,
    /// Valid confirmed bars whose close was ahead of local receipt time. Release waits for the
    /// actual close and preserves the original upstream receipt timestamp.
    pub deferred: VecDeque<DeferredCandle>,
    /// Local receipt time of the newest confirmed bar appended to `rows`.
    pub last_confirmed_at: f64,
}

#[derive(Clone, Debug)]
pub struct DeferredCandle {
    pub candle: Candle,
    pub received_at: f64,
}

pub const MAX_DEFERRED: usize = 8;
/// A confirmed bar whose close is further than this ahead of the local clock is not held:
/// it is dropped and its pair invalidated, because the clock is unusable rather than skewed.
pub const DEFERRAL_HORIZON_SECS: f64 = 180.0;

pub struct State {
    pub pairs: BTreeMap<String, PairState>,
    pub capacity: usize,
    pub epoch: String,
    pub events: broadcast::Sender<Event>,
    pub counters: BTreeMap<&'static str, u64>,
}
pub type Shared = Arc<Mutex<State>>;
impl State {
    pub fn new(pairs: &[String], capacity: usize) -> Shared {
        let (events, _) = broadcast::channel(BACKLOG);
        Arc::new(Mutex::new(Self {
            pairs: pairs
                .iter()
                .map(|p| {
                    (
                        p.clone(),
                        PairState {
                            rows: VecDeque::new(),
                            seed_base: Vec::new(),
                            live_head: None,
                            sequence: 0,
                            ready: false,
                            online: false,
                            generation: 0,
                            seeding: false,
                            received_at: 0.0,
                            reason: "starting".into(),
                            lanes: 0,
                            reconnects: 0,
                            rejected: BTreeMap::new(),
                            deferred: VecDeque::new(),
                            last_confirmed_at: 0.0,
                        },
                    )
                })
                .collect(),
            capacity,
            epoch: uuid::Uuid::new_v4().to_string(),
            events,
            counters: BTreeMap::new(),
        }))
    }
    pub fn count(&mut self, name: &'static str) {
        *self.counters.entry(name).or_default() += 1;
    }
    pub fn envelope(&self, kind: &str) -> Value {
        json!({"v":1,"type":kind,"epoch":self.epoch,"published_at":now()})
    }
    pub fn frame(&self, pair: &str, history: usize) -> Value {
        let p = &self.pairs[pair];
        let mut value = self.envelope(if p.ready { "snapshot" } else { "status" });
        value["pair"] = json!(pair);
        value["sequence"] = json!(p.sequence);
        if p.ready {
            value["received_at"] = json!(p.received_at);
            value["rows"] = json!(
                p.rows
                    .iter()
                    .skip(p.rows.len().saturating_sub(history))
                    .collect::<Vec<_>>()
            );
        } else {
            value["ready"] = json!(false);
            value["reason"] = json!(p.reason);
        }
        value
    }
    pub fn invalidate(&mut self, pair: &str, reason: &str) {
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        if !p.ready && p.reason == reason {
            return;
        }
        if p.ready && p.rows.len() == self.capacity {
            p.seed_base = p.rows.iter().cloned().collect();
        }
        p.ready = false;
        p.reason = reason.into();
        p.sequence += 1;
        let frame = self.frame(pair, self.capacity);
        let _ = self.events.send(Event::Other(frame));
        self.count("invalidations");
    }
    pub fn disconnected(&mut self, pairs: &[String], reason: &str) {
        self.count("disconnects");
        for pair in pairs {
            let p = self.pairs.get_mut(pair).unwrap();
            p.online = false;
            p.lanes = 0;
            p.deferred.clear();
            p.generation += 1;
            // Retain only a complete validated baseline, never readiness. REST and
            // current-generation socket rows take precedence over this baseline.
            if p.ready {
                p.seed_base = p.rows.iter().cloned().collect();
            }
            p.rows.clear();
            p.live_head = None;
            p.seeding = false;
            self.invalidate(pair, reason);
        }
    }
    pub fn reject_pair_data(&mut self, pair: &str) {
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        p.generation += 1;
        p.rows.clear();
        p.seed_base.clear();
        p.deferred.clear();
        p.live_head = None;
        p.seeding = false;
        self.invalidate(pair, "invalid_upstream_candle");
        self.count("parser_failures");
        self.note_rejected(pair, "malformed_confirmed");
    }
    pub fn acknowledge(&mut self, pair: &str) {
        self.acknowledge_lane(pair, 0);
    }
    pub fn acknowledge_lane(&mut self, pair: &str, lane: u8) {
        debug_assert!(lane < 8);
        let p = self.pairs.get_mut(pair).unwrap();
        p.lanes |= 1 << lane;
        p.online = true;
        self.try_warm_restore(pair);
    }
    /// One upstream lane ended. Pairs that another lane still serves stay online and ready;
    /// only pairs left with no live lane take the full disconnect path.
    pub fn lane_disconnected(&mut self, pairs: &[String], lane: u8, reason: &str) {
        self.count("lane_disconnects");
        let mut orphaned = Vec::new();
        for pair in pairs {
            let p = self.pairs.get_mut(pair).unwrap();
            if p.lanes & (1 << lane) != 0 {
                p.reconnects += 1;
            }
            p.lanes &= !(1 << lane);
            if p.lanes == 0 {
                orphaned.push(pair.clone());
            }
        }
        if !orphaned.is_empty() {
            self.disconnected(&orphaned, reason);
        }
    }
    /// Count a refused or ignored upstream row for a pair without disturbing its history.
    pub fn note_rejected(&mut self, pair: &str, reason: &'static str) {
        *self
            .pairs
            .get_mut(pair)
            .expect("catalog pair")
            .rejected
            .entry(reason)
            .or_default() += 1;
        self.count("rows_rejected");
    }
    /// Hold a valid confirmed bar that arrived before its close, retaining upstream receipt time.
    pub fn defer(&mut self, pair: &str, row: Candle, received_at: f64) {
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        if let Some(held) = p
            .deferred
            .iter_mut()
            .find(|held| held.candle.open_time == row.open_time)
        {
            if held.candle != row {
                held.candle = row;
                held.received_at = received_at;
            }
            return;
        }
        if p.deferred.len() == MAX_DEFERRED {
            p.deferred.pop_front();
        }
        p.deferred.push_back(DeferredCandle {
            candle: row,
            received_at,
        });
        p.deferred
            .make_contiguous()
            .sort_by_key(|r| r.candle.open_time);
        self.note_rejected(pair, "clock_skew_deferred");
    }
    /// Reconcile deferred bars with the (possibly corrected) local clock.
    pub fn release_deferred(&mut self, at: f64) {
        let names: Vec<_> = self
            .pairs
            .iter()
            .filter(|(_, p)| !p.deferred.is_empty())
            .map(|(name, _)| name.clone())
            .collect();
        for pair in names {
            let mut due = Vec::new();
            let mut expired = false;
            {
                let p = self.pairs.get_mut(&pair).unwrap();
                while let Some(front) = p.deferred.front() {
                    let close = (front.candle.open_time + 60) as f64;
                    if close <= at {
                        due.push(p.deferred.pop_front().unwrap());
                    } else if close - at > DEFERRAL_HORIZON_SECS {
                        // Local clock is beyond the deferral horizon: give up on this row.
                        p.deferred.pop_front();
                        expired = true;
                    } else {
                        break;
                    }
                }
            }
            for row in due {
                self.count("deferred_released");
                self.ingest_published(&pair, row.candle, row.received_at, at);
            }
            if expired {
                self.note_rejected(&pair, "clock_skew_unreconciled");
                self.invalidate(&pair, "clock_skew_unreconciled");
            }
        }
    }
    pub fn set_seed_base(&mut self, pair: &str, rows: Vec<Candle>) {
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        p.seed_base = rows.into_iter().map(Arc::new).collect();
        // A disk cache never changes online/ready/sequence, even if fresh.
    }
    pub fn observe_live_head(
        &mut self,
        pair: &str,
        open_time: u64,
        confirmed: bool,
        received_at: f64,
    ) {
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        if !p.ready {
            p.received_at = p.received_at.max(received_at);
        }
        if p.live_head
            .is_none_or(|(previous, _, _)| open_time >= previous)
        {
            p.live_head = Some((open_time, confirmed, received_at));
        }
        self.try_warm_restore(pair);
    }
    fn try_warm_restore(&mut self, pair: &str) {
        let p = &self.pairs[pair];
        if p.ready || !p.online || p.seed_base.len() != self.capacity {
            return;
        }
        let Some((head, confirmed, observed_at)) = p.live_head else {
            return;
        };
        if !current_live_head(head, confirmed, observed_at, now()) {
            return;
        }
        let mut merged: BTreeMap<_, _> = p
            .seed_base
            .iter()
            .map(|r| (r.open_time, r.clone()))
            .collect();
        for row in &p.rows {
            merged.insert(row.open_time, row.clone());
        }
        let n = merged.len();
        let rows: Vec<_> = merged
            .into_values()
            .skip(n.saturating_sub(self.capacity))
            .collect();
        let Some(last) = rows.last() else {
            return;
        };
        // Forming data is proof of the current live minute only; never a history row.
        let live_matches = if confirmed {
            head == last.open_time
        } else {
            head == last.open_time + 60
        };
        if live_matches
            && crate::model::history_refs_valid(
                rows.iter().map(|r| r.as_ref()),
                self.capacity,
                now(),
            )
        {
            let generation = p.generation;
            let received_at = p.received_at;
            self.apply_seed_shared(pair, generation, rows, received_at);
            self.count("warm_restores");
        }
    }
    pub fn ingest(&mut self, pair: &str, row: Candle, received_at: f64) {
        let close = (row.open_time + 60) as f64;
        if close <= received_at {
            let p = self.pairs.get_mut(pair).expect("catalog pair");
            if let Some(index) = p
                .deferred
                .iter()
                .position(|held| held.candle.open_time == row.open_time)
            {
                p.deferred.remove(index);
                self.count("deferred_superseded");
            }
            // Preserve candle order if a previous row is corrected just as a newer deferred
            // close becomes due. Older queued rows must precede this one; newer ones follow it.
            self.release_deferred_before(pair, row.open_time, received_at);
        }
        self.ingest_published(pair, row, received_at, now());
        if close <= received_at {
            self.release_deferred(received_at);
        }
    }
    fn release_deferred_before(&mut self, pair: &str, open_time: u64, at: f64) {
        let mut due = Vec::new();
        let p = self.pairs.get_mut(pair).expect("catalog pair");
        while p.deferred.front().is_some_and(|held| {
            held.candle.open_time < open_time && (held.candle.open_time + 60) as f64 <= at
        }) {
            due.push(p.deferred.pop_front().unwrap());
        }
        for row in due {
            self.count("deferred_released");
            self.ingest_published(pair, row.candle, row.received_at, at);
        }
    }
    fn ingest_published(&mut self, pair: &str, row: Candle, received_at: f64, published_at: f64) {
        self.count("confirmed_rows");
        let p = self.pairs.get_mut(pair).unwrap();
        if let Some(last) = p.rows.back() {
            if row.open_time < last.open_time {
                self.count("old_rows_ignored");
                return;
            }
            if row == **last {
                self.count("duplicates_ignored");
                return;
            }
            if row.open_time == last.open_time {
                p.rows.pop_back();
            } else if row.open_time != last.open_time + 60 {
                let p = self.pairs.get_mut(pair).unwrap();
                if p.ready {
                    p.seed_base = p.rows.iter().cloned().collect();
                }
                self.invalidate(pair, "candle_gap");
            }
        }
        let p = self.pairs.get_mut(pair).unwrap();
        p.rows.push_back(Arc::new(row.clone()));
        while p.rows.len() > self.capacity {
            p.rows.pop_front();
        }
        p.received_at = received_at;
        p.last_confirmed_at = received_at;
        if p.ready {
            p.sequence += 1;
            let sequence = p.sequence;
            let mut frame = self.envelope("candle");
            frame["published_at"] = json!(published_at);
            frame["pair"] = json!(pair);
            frame["sequence"] = json!(sequence);
            frame["received_at"] = json!(received_at);
            frame["row"] = json!(row);
            let _ = self.events.send(Event::Other(frame));
            self.count("candle_events");
        }
    }
    /// Returns true when the pair became ready from this seed.
    pub fn apply_seed(
        &mut self,
        pair: &str,
        generation: u64,
        rows: Vec<Candle>,
        received_at: f64,
    ) -> bool {
        self.apply_seed_shared(
            pair,
            generation,
            rows.into_iter().map(Arc::new).collect(),
            received_at,
        )
    }
    fn apply_seed_shared(
        &mut self,
        pair: &str,
        generation: u64,
        rows: Vec<Arc<Candle>>,
        received_at: f64,
    ) -> bool {
        let p = self.pairs.get_mut(pair).unwrap();
        if p.generation != generation || !p.online || p.ready {
            return false;
        }
        p.seeding = false;
        let mut merged: BTreeMap<u64, Arc<Candle>> =
            rows.into_iter().map(|r| (r.open_time, r)).collect();
        // Rows arriving over this live connection are authoritative over its older REST request.
        for row in &p.rows {
            merged.insert(row.open_time, row.clone());
        }
        let n = merged.len();
        let rows: Vec<_> = merged
            .into_values()
            .skip(n.saturating_sub(self.capacity))
            .collect();
        if !crate::model::history_refs_valid(rows.iter().map(|r| r.as_ref()), self.capacity, now())
        {
            self.invalidate(pair, "seed_not_contiguous_or_fresh");
            self.count("seed_failures");
            return false;
        }
        let p = self.pairs.get_mut(pair).unwrap();
        p.rows = rows.into();
        p.seed_base.clear();
        p.ready = true;
        p.reason.clear();
        p.sequence += 1;
        p.received_at = p.received_at.max(received_at);
        let p = &self.pairs[pair];
        let mut metadata = self.envelope("snapshot");
        metadata["pair"] = json!(pair);
        metadata["sequence"] = json!(p.sequence);
        metadata["received_at"] = json!(p.received_at);
        let _ = self.events.send(Event::Snapshot {
            metadata,
            rows: p.rows.iter().cloned().collect::<Vec<_>>().into(),
        });
        self.count("snapshots");
        true
    }
    pub fn health(&self) -> Value {
        json!({"v":1,"epoch":self.epoch,"published_at":now(),"history":self.capacity,"max_history":crate::model::MAX_HISTORY,"ready":self.pairs.values().all(|p| p.ready),"counters":self.counters,"pairs":self.pairs.iter().map(|(name,p)| (name,json!({"ready":p.ready,"online":p.online,"sequence":p.sequence,"rows":p.rows.len(),"reason":p.reason,"received_at":p.received_at,"last_open_time":p.rows.back().map(|r|r.open_time)}))).collect::<BTreeMap<_,_>>()})
    }
    /// Read-only per-pair freshness and comparison view for dashboards, push alarms and the
    /// active/active digest check. `window` rows from the newest confirmed bar are hashed with
    /// the same canonical JSON the warm-restart cache uses (see `window_digest`).
    pub fn metrics(&self, pairs: Option<&BTreeSet<String>>, window: usize, at: f64) -> Value {
        let window = window.clamp(2, self.capacity);
        let mut rejected_total: BTreeMap<&'static str, u64> = BTreeMap::new();
        let mut per_pair = BTreeMap::new();
        for (name, p) in &self.pairs {
            if pairs.is_some_and(|scope| !scope.contains(name)) {
                continue;
            }
            for (reason, count) in &p.rejected {
                *rejected_total.entry(reason).or_default() += count;
            }
            let tail = p.rows.back();
            let (digest, head) = if p.ready && p.rows.len() >= window {
                let tail_rows: Vec<&Candle> = p
                    .rows
                    .iter()
                    .skip(p.rows.len() - window)
                    .map(|r| r.as_ref())
                    .collect();
                (
                    Some(window_digest(tail_rows.iter().copied())),
                    tail_rows.last().map(|r| r.open_time),
                )
            } else {
                (None, None)
            };
            per_pair.insert(
                name.clone(),
                json!({
                    "ready": p.ready, "online": p.online, "lanes": p.lanes.count_ones(),
                    "reason": p.reason, "sequence": p.sequence, "rows": p.rows.len(),
                    "last_open_time": tail.map(|r| r.open_time),
                    "last_close_age_s": tail.map(|r| (at - (r.open_time + 60) as f64).max(0.0)),
                    "last_confirmed_receipt_age_s": (p.last_confirmed_at > 0.0).then(|| (at - p.last_confirmed_at).max(0.0)),
                    "reconnects": p.reconnects, "rejected": p.rejected,
                    "deferred": p.deferred.len(),
                    "window": window, "window_digest": digest, "window_head_open_time": head,
                }),
            );
        }
        json!({
            "v": 1, "schema": "okx-candle-service.metrics.v1", "epoch": self.epoch, "published_at": at,
            // Retain the v1 compatibility key, but report the now-active zero early-close tolerance.
            "history": self.capacity, "skew_tolerance_s": 0.0,
            "ready": per_pair.values().all(|p| p["ready"] == true),
            "counters": self.counters, "rejected_by_reason": rejected_total, "pairs": per_pair,
        })
    }
}

/// SHA-256 over the canonical JSON array of `rows`; identical to the cache file's digest when
/// given a complete history, so two instances' cache bytes and live windows are comparable.
pub fn window_digest<'a>(rows: impl IntoIterator<Item = &'a Candle>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"[");
    for (index, row) in rows.into_iter().enumerate() {
        if index > 0 {
            hasher.update(b",");
        }
        hasher.update(serde_json::to_vec(row).expect("candle serializes"));
    }
    hasher.update(b"]");
    format!("{:x}", hasher.finalize())
}

fn current_live_head(head: u64, confirmed: bool, received_at: f64, at: f64) -> bool {
    if !received_at.is_finite()
        || !at.is_finite()
        || received_at < 60.0
        || at < 60.0
        || received_at > at
    {
        return false;
    }
    let offset = if confirmed { 60 } else { 0 };
    let receipt_head = (received_at as u64 / 60) * 60 - offset;
    let current_head = (at as u64 / 60) * 60 - offset;
    head == receipt_head && head == current_head
}
#[cfg(test)]
mod warm_proof_tests {
    use super::*;
    #[test]
    fn previous_minute_head_and_head_crossing_ack_minute_cannot_warm_cache() {
        let minute = 1_800_000_000;
        let at = minute as f64 + 29.0;
        // Old cached tail closes 89 seconds ago: valid freshness alone is insufficient.
        let tail = Candle {
            open_time: minute - 120,
            open: "1".into(),
            high: "1".into(),
            low: "1".into(),
            close: "1".into(),
            volume: "0".into(),
            quote_volume: "0".into(),
            confirmed: true,
        };
        assert!(crate::model::history_valid(&[tail], 1, at));
        assert!(!current_live_head(minute - 60, false, at, at));
        assert!(!current_live_head(minute - 120, true, at, at));
        assert!(current_live_head(minute, false, at, at));
        assert!(current_live_head(minute - 60, true, at, at));
        assert!(!current_live_head(minute, false, at, minute as f64 + 60.1));
        assert!(!current_live_head(
            minute - 60,
            true,
            at,
            minute as f64 + 60.1
        ));
    }
}
