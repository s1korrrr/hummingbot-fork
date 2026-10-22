use okx_candle_service::{
    cache::Cache,
    history::HistoryPager,
    model::{Candle, now},
    state::State,
};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
fn rows(count: usize, end: u64) -> Vec<Candle> {
    (0..count)
        .map(|n| Candle {
            open_time: end - (count - 1 - n) as u64 * 60,
            open: "10".into(),
            high: "11".into(),
            low: "9".into(),
            close: "10".into(),
            volume: "1".into(),
            quote_volume: "10".into(),
            confirmed: true,
        })
        .collect()
}
fn latest() -> u64 {
    (now() as u64 / 60 - 1) * 60
}
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("okx-cache-test-{}", uuid::Uuid::new_v4())))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn cache_roundtrip_large_history_rejects_corruption_partial_stale_and_wrong_identity() {
    let temp = Temp::new();
    let cache = Cache::open(&temp.0).unwrap();
    let expected = rows(500, latest());
    cache.save("BTC-USDC", &expected).unwrap();
    assert_eq!(
        cache.load("BTC-USDC", 500, now()).unwrap(),
        Some(expected.clone())
    );
    let path = temp.0.join("BTC-USDC.json");
    let original = fs::read(&path).unwrap();
    let record: serde_json::Value = serde_json::from_slice(&original).unwrap();
    for kind in ["corrupt", "partial", "stale", "identity", "gap", "future"] {
        let mut bad = record.clone();
        match kind {
            "corrupt" => bad["rows"][0]["close"] = "10.5".into(),
            "partial" => {
                bad["rows"].as_array_mut().unwrap().pop();
            }
            "stale" => bad["saved_at"] = (now() - 86_401.0).into(),
            "identity" => bad["pair"] = "ETH-USDC".into(),
            "gap" => bad["rows"][0]["open_time"] = 1.into(),
            "future" => bad["saved_at"] = (now() + 60.0).into(),
            _ => unreachable!(),
        }
        fs::write(&path, serde_json::to_vec(&bad).unwrap()).unwrap();
        assert!(cache.load("BTC-USDC", 500, now()).is_err(), "{kind}");
    }
    fs::write(&path, b"{partial").unwrap();
    assert!(cache.load("BTC-USDC", 500, now()).is_err());
    fs::write(&path, &original).unwrap();
    assert!(cache.load("BTC-USDC", 501, now()).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(cache.load("BTC-USDC", 500, now()).is_err());
}
#[tokio::test]
async fn cached_history_never_seeds_offline_then_live_forming_head_restores_as_snapshot() {
    let shared = State::new(&["BTC-USDC".into()], 500);
    let expected = rows(500, latest());
    let head = latest() + 60;
    let mut s = shared.lock().await;
    let mut events = s.events.subscribe();
    s.set_seed_base("BTC-USDC", expected.clone());
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].sequence, 0);
    s.observe_live_head("BTC-USDC", head, false, now());
    assert!(!s.pairs["BTC-USDC"].ready);
    s.acknowledge("BTC-USDC");
    assert!(s.pairs["BTC-USDC"].ready);
    let frame = events.try_recv().unwrap().into_frame(500);
    assert_eq!(frame["type"], "snapshot");
    assert_eq!(frame["rows"].as_array().unwrap().len(), 500);
    assert_eq!(s.counters["warm_restores"], 1);
    assert!(!s.counters.contains_key("candle_events"));
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().unwrap().open_time,
        head - 60
    );
}
#[tokio::test]
async fn stale_cache_or_new_generation_requires_gap_reconciliation() {
    let shared = State::new(&["BTC-USDC".into()], 500);
    let mut s = shared.lock().await;
    let at = latest();
    s.set_seed_base("BTC-USDC", rows(500, at - 300));
    s.acknowledge("BTC-USDC");
    s.observe_live_head("BTC-USDC", at + 60, false, now());
    assert!(!s.pairs["BTC-USDC"].ready);
    s.apply_seed("BTC-USDC", 0, rows(500, at), now());
    assert!(s.pairs["BTC-USDC"].ready);
    s.disconnected(&["BTC-USDC".into()], "lost");
    s.acknowledge("BTC-USDC");
    assert!(!s.pairs["BTC-USDC"].ready);
    s.apply_seed("BTC-USDC", 0, rows(500, at), now());
    assert!(!s.pairs["BTC-USDC"].ready);
    s.observe_live_head("BTC-USDC", at + 60, false, now());
    assert!(s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].generation, 1);
}
#[test]
fn cold_history_paginates_and_warm_gap_fetches_only_missing_suffix() {
    let at = latest();
    let mut cold = HistoryPager::new(7500, Vec::new());
    let mut end = at;
    let mut count = 0;
    loop {
        let request = cold.request(now()).unwrap();
        assert!(request.limit <= 300);
        assert_eq!(
            request.after_ms,
            if count == 0 {
                None
            } else {
                Some((end + 60) * 1000)
            }
        );
        let page = rows(request.limit, end);
        end = page[0].open_time - 60;
        count += 1;
        if let Some(result) = cold.accept(page, now()).unwrap() {
            assert_eq!(result, rows(7500, at));
            break;
        }
    }
    assert_eq!(count, 25);
    let mut warm = HistoryPager::new(7500, rows(7500, at - 180));
    let request = warm.request(now()).unwrap();
    assert!(request.limit <= 6);
    assert_eq!(
        warm.accept(rows(request.limit, at), now())
            .unwrap()
            .unwrap(),
        rows(7500, at)
    );
}
#[test]
fn pagination_rejects_no_progress_and_missing_history() {
    let at = latest();
    let mut pager = HistoryPager::new(500, Vec::new());
    assert!(pager.accept(rows(300, at), now()).unwrap().is_none());
    assert!(pager.accept(rows(300, at), now()).is_err());
    let mut pager = HistoryPager::new(500, Vec::new());
    assert!(pager.accept(Vec::new(), now()).is_err());
    let mut bad = rows(300, at);
    bad.remove(100);
    let mut pager = HistoryPager::new(500, Vec::new());
    assert!(pager.accept(bad, now()).unwrap().is_none());
    assert!(pager.accept(rows(201, at - 300 * 60), now()).is_err());
}

#[test]
fn old_nonoverlapping_cache_uses_bounded_cold_history_and_stale_rest_rejected() {
    let at = latest();
    let mut pager = HistoryPager::new(500, rows(500, at - 1000 * 60));
    assert_eq!(pager.request(now()).unwrap().limit, 300);
    assert!(pager.accept(rows(300, at), now()).unwrap().is_none());
    assert_eq!(pager.request(now()).unwrap().limit, 201);
    assert_eq!(
        pager
            .accept(rows(201, at - 300 * 60), now())
            .unwrap()
            .unwrap(),
        rows(500, at)
    );
    let mut stale = HistoryPager::new(500, Vec::new());
    assert!(stale.accept(rows(300, at - 300), now()).is_err());
}

#[tokio::test]
async fn warm_cache_merge_keeps_live_correction_and_ignores_inflight_seed_after_restore() {
    let shared = State::new(&["BTC-USDC".into()], 500);
    let mut s = shared.lock().await;
    let at = latest();
    let baseline = rows(500, at);
    s.set_seed_base("BTC-USDC", baseline.clone());
    s.acknowledge("BTC-USDC");
    let mut correction = baseline.last().unwrap().clone();
    correction.close = "10.5".into();
    s.ingest("BTC-USDC", correction.clone(), now());
    s.observe_live_head("BTC-USDC", at, true, now());
    assert!(s.pairs["BTC-USDC"].ready);
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().map(|r| r.as_ref()),
        Some(&correction)
    );
    let seq = s.pairs["BTC-USDC"].sequence;
    s.apply_seed("BTC-USDC", 0, baseline, now());
    assert_eq!(s.pairs["BTC-USDC"].sequence, seq);
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().map(|r| r.as_ref()),
        Some(&correction)
    );
}

#[tokio::test]
async fn recovery_snapshots_share_immutable_rows_and_do_not_absorb_later_corrections() {
    use okx_candle_service::state::Event;
    use std::sync::Arc;
    let shared = State::new(&["BTC-USDC".into()], 500);
    let mut s = shared.lock().await;
    let at = latest();
    let mut events = s.events.subscribe();
    s.acknowledge("BTC-USDC");
    s.apply_seed("BTC-USDC", 0, rows(500, at), now());
    let first = events.try_recv().unwrap();
    let Event::Snapshot {
        rows: first_rows, ..
    } = first
    else {
        panic!("snapshot expected")
    };
    assert!(Arc::ptr_eq(&first_rows[0], &s.pairs["BTC-USDC"].rows[0]));
    s.disconnected(&["BTC-USDC".into()], "lost");
    let _ = events.try_recv().unwrap();
    s.acknowledge("BTC-USDC");
    s.observe_live_head("BTC-USDC", at + 60, false, now());
    let second = events.try_recv().unwrap();
    let Event::Snapshot {
        rows: second_rows, ..
    } = second
    else {
        panic!("snapshot expected")
    };
    assert!(Arc::ptr_eq(&first_rows[0], &second_rows[0]));
    let mut correction = rows(1, at).pop().unwrap();
    correction.close = "10.5".into();
    s.ingest("BTC-USDC", correction, now());
    assert_eq!(first_rows.last().unwrap().close, "10");
    assert_eq!(second_rows.last().unwrap().close, "10");
    assert_eq!(s.pairs["BTC-USDC"].rows.back().unwrap().close, "10.5");
}

#[tokio::test]
async fn shutdown_flush_persists_latest_unsaved_candle_after_collector_disconnect() {
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    let temp = Temp::new();
    let cache = Cache::open(&temp.0).unwrap();
    let state = State::new(&["BTC-USDC".into()], 500);
    let at = latest();
    {
        let mut s = state.lock().await;
        s.acknowledge("BTC-USDC");
        s.apply_seed("BTC-USDC", 0, rows(500, at), now());
    }
    let stop = CancellationToken::new();
    let writer = tokio::spawn(cache.clone().run(state.clone(), stop.clone()));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if state.lock().await.counters.get("cache_writes") == Some(&1) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut s = state.lock().await;
        // A latest-bar correction is a new confirmed update within the same minute;
        // this remains deterministic without waiting for a wall-clock minute boundary.
        let mut updated = rows(1, at).pop().unwrap();
        updated.close = "10.5".into();
        s.ingest("BTC-USDC", updated, now());
        s.disconnected(&["BTC-USDC".into()], "shutdown");
        assert!(!s.pairs["BTC-USDC"].ready);
        assert!(s.pairs["BTC-USDC"].rows.is_empty());
    }
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(2), writer)
        .await
        .unwrap()
        .unwrap();
    let disk = cache.load("BTC-USDC", 500, now()).unwrap().unwrap();
    assert_eq!(disk.last().unwrap().close, "10.5");
    let s = state.lock().await;
    assert!(!s.pairs["BTC-USDC"].online);
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.counters["cache_writes"], 2);
}
#[tokio::test]
async fn replayed_prior_minute_forming_head_cannot_restore_cached_readiness() {
    let state = State::new(&["BTC-USDC".into()], 500);
    let at = latest();
    let mut s = state.lock().await;
    s.set_seed_base("BTC-USDC", rows(500, at - 60));
    s.acknowledge("BTC-USDC");
    s.observe_live_head("BTC-USDC", at, false, now());
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].sequence, 0);
    assert!(!s.counters.contains_key("warm_restores"));
}
