//! Bounded REST pagination and deterministic cache/REST merge planning.
use crate::model::{Candle, MAX_HISTORY, history_valid};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;

pub const PAGE_SIZE: usize = 300;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    pub after_ms: Option<u64>,
    pub limit: usize,
}

pub struct HistoryPager {
    rows: BTreeMap<u64, Candle>,
    capacity: usize,
    anchor: Option<u64>,
    cursor: Option<u64>,
    pages: usize,
    overlap: bool,
    initial_at: Option<f64>,
}
impl HistoryPager {
    pub fn new(capacity: usize, cached: Vec<Candle>) -> Self {
        let cached = if cached.last().is_some_and(|r| {
            crate::model::now() - (r.open_time + 60) as f64 >= capacity as f64 * 60.0
        }) {
            Vec::new()
        } else {
            cached
        };
        let anchor = cached.last().map(|r| r.open_time);
        Self {
            rows: cached.into_iter().map(|r| (r.open_time, r)).collect(),
            capacity,
            anchor,
            cursor: None,
            pages: 0,
            overlap: false,
            initial_at: None,
        }
    }
    pub fn request(&self, at: f64) -> Result<PageRequest> {
        ensure!(
            (2..=MAX_HISTORY).contains(&self.capacity),
            "history outside bounded capacity"
        );
        ensure!(
            self.pages < self.capacity.div_ceil(PAGE_SIZE) + 2,
            "REST history page budget exhausted"
        );
        let remaining = if self.pages == 0 {
            self.anchor
                .map(|anchor| ((at.max(0.0) as u64 / 60).saturating_sub(anchor / 60) + 2) as usize)
                .unwrap_or(self.capacity + 2)
        } else if let (Some(cursor), Some(anchor)) = (self.cursor, self.anchor) {
            if cursor > anchor {
                (cursor - anchor) as usize / 60 + 2
            } else {
                self.capacity.saturating_sub(self.rows.len()).max(1) + 1
            }
        } else {
            self.capacity.saturating_sub(self.rows.len()).max(1) + 1
        };
        Ok(PageRequest {
            after_ms: self.cursor.map(|t| t * 1000),
            limit: remaining.clamp(3, PAGE_SIZE),
        })
    }
    pub fn accept(&mut self, page: Vec<Candle>, at: f64) -> Result<Option<Vec<Candle>>> {
        ensure!(
            !page.is_empty() && page.len() <= PAGE_SIZE,
            "REST page must contain confirmed rows"
        );
        if self.initial_at.is_none() {
            let newest = page.iter().map(|r| r.open_time).max().unwrap();
            ensure!(
                at - (newest + 60) as f64 <= 90.0 && (newest + 60) as f64 <= at,
                "initial REST page is stale or future"
            );
            self.initial_at = Some(at);
        }
        let oldest = page.iter().map(|r| r.open_time).min().unwrap();
        ensure!(
            self.cursor.is_none_or(|cursor| oldest < cursor),
            "REST pagination made no progress"
        );
        self.overlap |= self.anchor.is_none_or(|anchor| oldest <= anchor);
        for row in page {
            self.rows.insert(row.open_time, row);
        }
        self.pages += 1;
        self.cursor = Some(oldest);
        while self.rows.len() > self.capacity {
            self.rows.pop_first();
        }
        let rows: Vec<_> = self.rows.values().cloned().collect();
        if self.overlap && history_valid(&rows, self.capacity, self.initial_at.unwrap()) {
            return Ok(Some(rows));
        }
        // Once we have the complete window, fetching older pages cannot repair a hole
        // inside it. Reject instead of silently returning a shorter history.
        ensure!(
            self.rows.len() < self.capacity || !self.overlap,
            "REST history has a gap or stale tail"
        );
        Ok(None)
    }
}
