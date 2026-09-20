use crate::model::{Candle, now};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
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
}

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
        p.live_head = None;
        p.seeding = false;
        self.invalidate(pair, "invalid_upstream_candle");
        self.count("parser_failures");
    }
    pub fn acknowledge(&mut self, pair: &str) {
        self.pairs.get_mut(pair).unwrap().online = true;
        self.try_warm_restore(pair);
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
        if p.ready {
            p.sequence += 1;
            let sequence = p.sequence;
            let mut frame = self.envelope("candle");
            frame["pair"] = json!(pair);
            frame["sequence"] = json!(sequence);
            frame["received_at"] = json!(received_at);
            frame["row"] = json!(row);
            let _ = self.events.send(Event::Other(frame));
            self.count("candle_events");
        }
    }
    pub fn apply_seed(&mut self, pair: &str, generation: u64, rows: Vec<Candle>, received_at: f64) {
        self.apply_seed_shared(
            pair,
            generation,
            rows.into_iter().map(Arc::new).collect(),
            received_at,
        );
    }
    fn apply_seed_shared(
        &mut self,
        pair: &str,
        generation: u64,
        rows: Vec<Arc<Candle>>,
        received_at: f64,
    ) {
        let p = self.pairs.get_mut(pair).unwrap();
        if p.generation != generation || !p.online || p.ready {
            return;
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
            return;
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
    }
    pub fn health(&self) -> Value {
        json!({"v":1,"epoch":self.epoch,"published_at":now(),"history":self.capacity,"max_history":crate::model::MAX_HISTORY,"ready":self.pairs.values().all(|p| p.ready),"counters":self.counters,"pairs":self.pairs.iter().map(|(name,p)| (name,json!({"ready":p.ready,"online":p.online,"sequence":p.sequence,"rows":p.rows.len(),"reason":p.reason,"received_at":p.received_at,"last_open_time":p.rows.back().map(|r|r.open_time)}))).collect::<BTreeMap<_,_>>()})
    }
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
