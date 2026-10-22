//! Contract tests for the hardening changes: tolerant timing, lane bookkeeping, deferred
//! reconciliation, backoff, digests, consumer scopes and metrics.
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use okx_candle_service::{
    auth::{Auth, Consumer},
    collector::{reconnect_delay, seed_retry_delay, unit_random},
    config::Args,
    history::{HistoryPager, StaleRestPage},
    model::{CLOCK_SKEW_SECS, Candle, Parsed, now, parse_row, parse_row_tolerant},
    server,
    state::{DEFERRAL_HORIZON_SECS, Event, State, window_digest},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, os::unix::fs::PermissionsExt, time::Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;

fn candle(open_time: u64) -> Candle {
    Candle {
        open_time,
        open: "10".into(),
        high: "11".into(),
        low: "9".into(),
        close: "10".into(),
        volume: "1".into(),
        quote_volume: "10".into(),
        confirmed: true,
    }
}
fn raw(open_time: u64, confirmed: bool) -> Value {
    json!([
        (open_time * 1000).to_string(),
        "10",
        "11",
        "9",
        "10",
        "1",
        "1",
        "10",
        if confirmed { "1" } else { "0" }
    ])
}
const MINUTE: u64 = 1_800_000_000; // minute-aligned fixed instant

#[test]
fn forming_rows_never_fail_even_with_a_far_behind_clock_but_confirmed_defects_do() {
    let received_at = MINUTE as f64 + 10.0;
    assert_eq!(
        parse_row_tolerant(&raw(MINUTE, false), received_at).unwrap(),
        Parsed::Forming(MINUTE)
    );
    // Forming bar a minute ahead of the local clock: the production "future candle" failure.
    assert_eq!(
        parse_row_tolerant(&raw(MINUTE + 60, false), received_at).unwrap(),
        Parsed::Ignored("forming_future")
    );
    let mut malformed = raw(MINUTE, false);
    malformed[2] = json!("5"); // high below open/close
    assert_eq!(
        parse_row_tolerant(&malformed, received_at).unwrap(),
        Parsed::Ignored("forming_malformed")
    );
    // A confirmed row stays strictly validated.
    let mut bad_confirmed = raw(MINUTE - 60, true);
    bad_confirmed[2] = json!("5");
    assert!(parse_row_tolerant(&bad_confirmed, received_at).is_err());
    let mut bad_flag = raw(MINUTE - 60, true);
    bad_flag[8] = json!("2");
    assert!(parse_row_tolerant(&bad_flag, received_at).is_err());
    assert!(parse_row_tolerant(&json!(["1"]), received_at).is_err());
}

#[test]
fn confirmed_rows_are_deferred_until_the_exact_close_boundary() {
    let open = MINUTE - 60; // closes at MINUTE
    let close = MINUTE as f64;
    let ok = |at: f64| {
        matches!(
            parse_row_tolerant(&raw(open, true), at).unwrap(),
            Parsed::Confirmed(_)
        )
    };
    assert!(ok(close));
    assert!(!ok(close - 0.1));
    assert!(!ok(close - CLOCK_SKEW_SECS));
    assert!(ok(close));
    assert!(matches!(
        parse_row_tolerant(&raw(open, true), close - 30.0).unwrap(),
        Parsed::Deferred(c) if c.open_time == open
    ));
    // Even a clock 50 s behind yields a classified deferral, never an error.
    assert!(matches!(
        parse_row_tolerant(&raw(open, true), close - 50.0).unwrap(),
        Parsed::Deferred(_)
    ));
    assert!(
        matches!(
            parse_row_tolerant(&raw(open - 60, true), close + 60.0).unwrap(),
            Parsed::Confirmed(_)
        ),
        "ordinary next-minute close is admitted"
    );
    // The strict legacy contract (cache revalidation, shared fixture) is unchanged.
    assert!(parse_row(&raw(open, true), close - 0.01).is_err());
    assert!(parse_row(&raw(open, true), close).unwrap().is_some());
}

#[tokio::test]
async fn rest_and_seed_reject_future_closes_but_accept_at_the_close_boundary() {
    let newest = MINUTE - 60;
    let page = || vec![candle(newest - 60), candle(newest)];
    // 91 s after the newest close: stale, typed so the repair loop can retry quickly.
    let mut pager = HistoryPager::new(2, Vec::new());
    let error = pager
        .accept(page(), (newest + 60) as f64 + 91.0)
        .unwrap_err();
    assert!(error.downcast_ref::<StaleRestPage>().is_some(), "{error:#}");
    assert_eq!(error.to_string(), "initial REST page is stale or future");
    // A confirmed history row before close is never seed-eligible, even within old skew.
    let mut pager = HistoryPager::new(2, Vec::new());
    let error = pager
        .accept(page(), (newest + 60) as f64 - 0.1)
        .unwrap_err();
    assert!(error.downcast_ref::<StaleRestPage>().is_some());
    let mut pager = HistoryPager::new(2, Vec::new());
    assert!(
        pager
            .accept(page(), (newest + 60) as f64)
            .unwrap()
            .is_some()
    );

    // A locally constructed seed carrying a future tail also fails the same strict boundary.
    let state = ready_state("BTC-USDC", 2);
    let mut s = state.lock().await;
    s.acknowledge("BTC-USDC");
    let this_minute = (now() as u64 / 60) * 60;
    assert!(!s.apply_seed(
        "BTC-USDC",
        0,
        vec![candle(this_minute - 60), candle(this_minute)],
        now()
    ));
    assert_eq!(s.pairs["BTC-USDC"].reason, "seed_not_contiguous_or_fresh");
}

#[test]
fn reconnect_and_seed_retry_backoff_are_exponential_capped_and_jittered() {
    let mut previous = Duration::ZERO;
    for attempt in 0..10u32 {
        let low = reconnect_delay(attempt, 0.0);
        let high = reconnect_delay(attempt, 0.999);
        let ceiling = f64::from(2u32.pow(attempt.min(5))).min(30.0);
        assert!(
            (low.as_secs_f64() - ceiling * 0.5).abs() < 1e-9,
            "{attempt} {low:?}"
        );
        assert!(high.as_secs_f64() < ceiling && high.as_secs_f64() > ceiling * 0.99);
        assert!(low >= previous.mul_f64(0.0) && high <= Duration::from_secs(30));
        previous = low;
    }
    assert!(reconnect_delay(9, 0.5) <= Duration::from_secs(30));
    assert!(seed_retry_delay(1, true, 0.5) < seed_retry_delay(1, false, 0.5));
    assert!(seed_retry_delay(1, false, 0.5) < seed_retry_delay(3, false, 0.5));
    assert!(seed_retry_delay(50, false, 0.999) <= Duration::from_secs_f64(60.0 * 1.25));
    let draws: BTreeSet<u64> = (0..200)
        .map(|_| unit_random())
        .inspect(|u| assert!((0.0..1.0).contains(u)))
        .map(f64::to_bits)
        .collect();
    assert!(draws.len() > 190, "jitter source must not be constant");
}

fn ready_state(pair: &str, capacity: usize) -> okx_candle_service::state::Shared {
    State::new(&[pair.to_string()], capacity)
}

#[tokio::test]
async fn lane_accounting_keeps_a_pair_live_until_its_last_lane_drops() {
    let state = ready_state("BTC-USDC", 2);
    let closed = (now() as u64 / 60 - 1) * 60;
    let mut s = state.lock().await;
    s.acknowledge_lane("BTC-USDC", 0);
    s.acknowledge_lane("BTC-USDC", 1);
    assert!(s.apply_seed(
        "BTC-USDC",
        0,
        vec![candle(closed - 60), candle(closed)],
        now()
    ));
    let pairs = vec!["BTC-USDC".to_string()];
    s.lane_disconnected(&pairs, 1, "upstream_disconnected");
    assert!(s.pairs["BTC-USDC"].online && s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].reconnects, 1);
    assert_eq!(
        s.pairs["BTC-USDC"].generation, 0,
        "in-flight seeds stay valid"
    );
    s.lane_disconnected(&pairs, 0, "upstream_disconnected");
    assert!(!s.pairs["BTC-USDC"].online && !s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].generation, 1);
    assert_eq!(s.counters["lane_disconnects"], 2);
    // A lane that was never acknowledged does not count as a reconnect of that lane.
    s.lane_disconnected(&pairs, 1, "upstream_disconnected");
    assert_eq!(s.pairs["BTC-USDC"].reconnects, 2);
}

#[tokio::test]
async fn deferred_bars_reconcile_when_the_clock_catches_up_and_expire_beyond_the_horizon() {
    let state = ready_state("BTC-USDC", 2);
    let base = (now() as u64 / 60 - 1) * 60; // newest closed minute: a fresh, valid seed
    let mut s = state.lock().await;
    let mut events = s.events.subscribe();
    s.acknowledge("BTC-USDC");
    assert!(s.apply_seed("BTC-USDC", 0, vec![candle(base - 60), candle(base)], now()));
    let _snapshot = events.try_recv().unwrap();
    let next = candle(base + 60); // closes at base + 120, ahead of the local clock
    let original_received_at = (base + 120) as f64 - 0.1;
    s.defer("BTC-USDC", next.clone(), original_received_at);
    s.defer("BTC-USDC", next.clone(), original_received_at + 0.05); // duplicate defer is idempotent
    assert_eq!(s.pairs["BTC-USDC"].deferred.len(), 1);
    // Local clock still 10 s short of close: stays held, nothing ingested.
    s.release_deferred((base + 110) as f64);
    assert_eq!(s.pairs["BTC-USDC"].deferred.len(), 1);
    assert_eq!(s.pairs["BTC-USDC"].rows.back().unwrap().open_time, base);
    // An early duplicate remains one queued row and cannot publish before close.
    s.defer("BTC-USDC", next.clone(), original_received_at);
    s.release_deferred((base + 120) as f64 - 0.001);
    assert_eq!(s.pairs["BTC-USDC"].deferred.len(), 1);
    assert_eq!(s.pairs["BTC-USDC"].rows.back().unwrap().open_time, base);
    // The periodic repair tick releases it at close without another upstream event.
    s.release_deferred((base + 120) as f64);
    assert!(s.pairs["BTC-USDC"].deferred.is_empty());
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().unwrap().open_time,
        base + 60
    );
    assert_eq!(s.counters["deferred_released"], 1);
    assert_eq!(s.counters["candle_events"], 1);
    assert_eq!(s.pairs["BTC-USDC"].last_confirmed_at, original_received_at);
    let published = events.try_recv().unwrap();
    assert!(matches!(published, Event::Other(_)));
    assert_eq!(
        published.metadata()["received_at"],
        json!(original_received_at)
    );
    assert_eq!(
        published.metadata()["published_at"],
        json!((base + 120) as f64)
    );
    // A bar further ahead than the horizon is not held: it is dropped and the pair invalidated.
    let far = candle(base + 120);
    let at = (far.open_time + 60) as f64 - DEFERRAL_HORIZON_SECS - 1.0;
    s.defer("BTC-USDC", far, at);
    s.release_deferred(at);
    assert!(s.pairs["BTC-USDC"].deferred.is_empty());
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].reason, "clock_skew_unreconciled");
}

#[tokio::test]
async fn post_close_row_supersedes_queued_early_row_before_periodic_release() {
    let state = ready_state("BTC-USDC", 2);
    let base = (now() as u64 / 60 - 1) * 60;
    let mut s = state.lock().await;
    s.acknowledge("BTC-USDC");
    assert!(s.apply_seed("BTC-USDC", 0, vec![candle(base - 60), candle(base)], now()));

    let open = base + 60;
    let close = (open + 60) as f64;
    let early = candle(open);
    s.defer("BTC-USDC", early, close - 0.1);
    let mut corrected = candle(open);
    corrected.close = "10.5".into();

    // The normal post-close corrected frame arrives before the repair timer. It supersedes the
    // queued early value directly; the later timer has nothing to release or overwrite.
    s.ingest("BTC-USDC", corrected.clone(), close + 0.001);
    assert!(s.pairs["BTC-USDC"].deferred.is_empty());
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().unwrap().as_ref(),
        &corrected
    );
    assert_eq!(s.counters.get("deferred_released"), None);
    assert_eq!(s.counters["deferred_superseded"], 1);
    assert_eq!(s.counters["candle_events"], 1);

    s.release_deferred(close + 1.0);
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().unwrap().as_ref(),
        &corrected
    );
    assert_eq!(s.counters["candle_events"], 1);

    // The ordinary next-minute close continues from the corrected row without a gap.
    let next = candle(open + 60);
    s.ingest("BTC-USDC", next.clone(), close + 60.0);
    assert_eq!(s.pairs["BTC-USDC"].rows.back().unwrap().as_ref(), &next);
    assert!(s.pairs["BTC-USDC"].ready);
    assert_eq!(s.counters["candle_events"], 2);
}

#[tokio::test]
async fn preceding_correction_is_published_before_a_newer_due_deferred_bar() {
    let state = ready_state("BTC-USDC", 2);
    let base = (now() as u64 / 60 - 1) * 60;
    let mut s = state.lock().await;
    let mut events = s.events.subscribe();
    s.acknowledge("BTC-USDC");
    assert!(s.apply_seed("BTC-USDC", 0, vec![candle(base - 60), candle(base)], now()));
    let _snapshot = events.try_recv().unwrap();

    let newer = candle(base + 60);
    let close = (newer.open_time + 60) as f64;
    s.defer("BTC-USDC", newer.clone(), close - 0.1);
    let mut correction = candle(base);
    correction.close = "10.5".into();
    // The timer has not fired, but the next candle is due. The previous candle is still the
    // latest published row and a correction must be applied before publishing the newer one.
    s.ingest("BTC-USDC", correction.clone(), close);
    assert_eq!(s.pairs["BTC-USDC"].rows[0].as_ref(), &correction);
    assert_eq!(s.pairs["BTC-USDC"].rows[1].as_ref(), &newer);
    assert_eq!(s.counters["candle_events"], 2);
    assert_eq!(
        events.try_recv().unwrap().metadata()["row"]["open_time"],
        json!(base)
    );
    assert_eq!(
        events.try_recv().unwrap().metadata()["row"]["open_time"],
        json!(base + 60)
    );
}

#[tokio::test]
async fn due_older_deferred_bar_precedes_a_later_post_close_row() {
    let state = ready_state("BTC-USDC", 2);
    let base = (now() as u64 / 60 - 1) * 60;
    let mut s = state.lock().await;
    let mut events = s.events.subscribe();
    s.acknowledge("BTC-USDC");
    assert!(s.apply_seed("BTC-USDC", 0, vec![candle(base - 60), candle(base)], now()));
    let _snapshot = events.try_recv().unwrap();

    let older = candle(base + 60);
    s.defer(
        "BTC-USDC",
        older.clone(),
        (older.open_time + 60) as f64 - 0.1,
    );
    let later = candle(base + 120);
    s.ingest("BTC-USDC", later.clone(), (later.open_time + 60) as f64);
    assert!(s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].rows[0].as_ref(), &older);
    assert_eq!(s.pairs["BTC-USDC"].rows[1].as_ref(), &later);
    assert_eq!(s.counters["candle_events"], 2);
    assert_eq!(
        events.try_recv().unwrap().metadata()["row"]["open_time"],
        json!(base + 60)
    );
    assert_eq!(
        events.try_recv().unwrap().metadata()["row"]["open_time"],
        json!(base + 120)
    );
}

#[tokio::test]
async fn malformed_confirmed_input_discards_earlier_deferred_rows_before_reseed() {
    let state = ready_state("BTC-USDC", 2);
    let base = (now() as u64 / 60 - 1) * 60;
    let mut s = state.lock().await;
    s.acknowledge("BTC-USDC");
    assert!(s.apply_seed("BTC-USDC", 0, vec![candle(base - 60), candle(base)], now()));
    let next = candle(base + 60);
    s.defer("BTC-USDC", next.clone(), (next.open_time + 60) as f64 - 0.1);

    s.reject_pair_data("BTC-USDC");
    assert!(s.pairs["BTC-USDC"].deferred.is_empty());
    s.release_deferred((next.open_time + 60) as f64);
    assert!(s.pairs["BTC-USDC"].rows.is_empty());
    assert!(s.apply_seed("BTC-USDC", 1, vec![candle(base - 60), candle(base)], now()));
    assert_eq!(s.pairs["BTC-USDC"].rows.back().unwrap().open_time, base);
}

#[tokio::test]
async fn later_early_correction_replaces_the_same_open_deferred_value_and_receipt() {
    let state = ready_state("BTC-USDC", 2);
    let mut s = state.lock().await;
    let open = MINUTE;
    let close = (open + 60) as f64;
    let first = candle(open);
    let mut correction = candle(open);
    correction.close = "10.5".into();
    s.defer("BTC-USDC", first, close - 0.2);
    s.defer("BTC-USDC", correction.clone(), close - 0.1);
    assert_eq!(s.pairs["BTC-USDC"].deferred.len(), 1);
    assert_eq!(s.pairs["BTC-USDC"].deferred[0].candle, correction);
    assert_eq!(s.pairs["BTC-USDC"].deferred[0].received_at, close - 0.1);

    s.release_deferred(close);
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().unwrap().as_ref(),
        &correction
    );
    assert_eq!(s.pairs["BTC-USDC"].last_confirmed_at, close - 0.1);
    assert_eq!(s.counters["deferred_released"], 1);
}

#[tokio::test]
async fn rejection_accounting_does_not_touch_the_warm_baseline() {
    let state = State::new(&["BTC-USDC".into(), "ETH-USDC".into()], 2);
    let mut s = state.lock().await;
    s.set_seed_base("BTC-USDC", vec![candle(MINUTE - 120), candle(MINUTE - 60)]);
    s.note_rejected("BTC-USDC", "forming_future");
    s.defer("BTC-USDC", candle(MINUTE), MINUTE as f64 - 0.1);
    assert_eq!(s.pairs["BTC-USDC"].seed_base.len(), 2);
    assert_eq!(s.pairs["BTC-USDC"].generation, 0);
    // Malformed confirmed data remains the one thing that drops the baseline.
    s.reject_pair_data("BTC-USDC");
    assert!(s.pairs["BTC-USDC"].seed_base.is_empty());
    assert_eq!(s.pairs["BTC-USDC"].rejected["malformed_confirmed"], 1);
}

#[tokio::test]
async fn window_digest_matches_cache_bytes_and_flags_a_single_changed_bar() {
    let rows: Vec<Candle> = (0..5).map(|i| candle(MINUTE + 60 * i)).collect();
    let canonical = format!("{:x}", Sha256::digest(serde_json::to_vec(&rows).unwrap()));
    assert_eq!(
        window_digest(rows.iter()),
        canonical,
        "same canonical bytes as the cache file digest"
    );
    let mut changed = rows.clone();
    changed[3].close = "10.1".into();
    assert_ne!(window_digest(changed.iter()), canonical);
    assert_eq!(
        window_digest(std::iter::empty()),
        format!("{:x}", Sha256::digest(b"[]"))
    );

    // Two instances holding identical confirmed history report the same window digest and
    // head; one differing bar changes the digest of any window that covers it.
    let closed = (now() as u64 / 60 - 1) * 60;
    let seed = |tweak: bool| {
        let rows: Vec<Candle> = (0..4)
            .map(|i| {
                let mut c = candle(closed - 60 * (3 - i));
                if tweak && i == 2 {
                    c.close = "10.5".into();
                }
                c
            })
            .collect();
        rows
    };
    let a = State::new(&["BTC-USDC".into()], 4);
    let b = State::new(&["BTC-USDC".into()], 4);
    let c = State::new(&["BTC-USDC".into()], 4);
    for (state, rows) in [(&a, seed(false)), (&b, seed(false)), (&c, seed(true))] {
        let mut s = state.lock().await;
        s.acknowledge("BTC-USDC");
        assert!(s.apply_seed("BTC-USDC", 0, rows, now()));
    }
    let at = now();
    let metrics = |state: &okx_candle_service::state::Shared| {
        let state = state.clone();
        async move { state.lock().await.metrics(None, 3, at) }
    };
    let (ma, mb, mc) = (metrics(&a).await, metrics(&b).await, metrics(&c).await);
    assert_eq!(
        ma["pairs"]["BTC-USDC"]["window_digest"],
        mb["pairs"]["BTC-USDC"]["window_digest"]
    );
    assert_ne!(
        ma["pairs"]["BTC-USDC"]["window_digest"],
        mc["pairs"]["BTC-USDC"]["window_digest"]
    );
    assert_eq!(
        ma["pairs"]["BTC-USDC"]["window_head_open_time"],
        json!(closed)
    );
    let age = ma["pairs"]["BTC-USDC"]["last_close_age_s"]
        .as_f64()
        .unwrap();
    assert!(
        (0.0..61.0).contains(&age),
        "newest closed bar is at most a minute old: {age}"
    );
    assert_eq!(ma["skew_tolerance_s"], json!(0.0));
}

async fn serve_with(
    auth: Auth,
    state: okx_candle_service::state::Shared,
) -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router_with_auth(state, auth, stop.clone(), None);
    let stop_server = stop.clone();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(stop_server.cancelled_owned())
            .await
            .unwrap();
    });
    (addr.to_string(), stop, handle)
}
fn request(addr: &str, token: &str) -> tokio_tungstenite::tungstenite::handshake::client::Request {
    let mut request = format!("ws://{addr}/v1/stream")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}
/// A just-closed stream releases its permit a moment after the client sees the close.
async fn connect_retry(
    addr: &str,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    for _ in 0..40 {
        if let Ok((socket, _)) = connect_async(request(addr, token)).await {
            return socket;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("stream was never admitted");
}
fn ended(item: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>) -> bool {
    matches!(item, None | Some(Err(_)) | Some(Ok(Message::Close(_))))
}
async fn health_counter(client: &reqwest::Client, addr: &str, token: &str, name: &str) -> u64 {
    let value: Value = client
        .get(format!("http://{addr}/health"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    value["counters"][name].as_u64().unwrap_or(0)
}

#[tokio::test]
async fn consumer_tokens_enforce_pairs_history_stream_caps_and_scope_the_metrics_view() {
    let v1 = "v1_token_0123456789abcdef0123456789abcdef";
    let v2 = "v2_token_0123456789abcdef0123456789abcdef";
    let pairs: Vec<String> = vec!["BTC-USDC".into(), "ETH-USDC".into()];
    let state = State::new(&pairs, 3);
    let closed = (now() as u64 / 60 - 1) * 60;
    {
        let mut s = state.lock().await;
        for pair in &pairs {
            s.acknowledge(pair);
            assert!(s.apply_seed(
                pair,
                0,
                vec![candle(closed - 120), candle(closed - 60), candle(closed)],
                now()
            ));
        }
    }
    let auth = Auth::new(vec![
        Consumer::new("v1", v1, Some(["BTC-USDC".to_string()].into()), 2, 1),
        Consumer::new("v2", v2, None, 3, 2),
    ]);
    let (addr, stop, handle) = serve_with(auth, state.clone()).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    // Authentication: unknown token, no token, browser origin.
    for token in [None, Some("x".repeat(40))] {
        let mut req = client.get(format!("http://{addr}/v1/metrics?window=2"));
        if let Some(token) = &token {
            req = req.bearer_auth(token);
        }
        assert_eq!(req.send().await.unwrap().status(), 401);
    }
    assert_eq!(
        client
            .get(format!("http://{addr}/v1/metrics"))
            .bearer_auth(v1)
            .header("origin", "https://x.invalid")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("http://{addr}/v1/metrics?window=1"))
            .bearer_auth(v1)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .get(format!("http://{addr}/v1/metrics?bogus=1"))
            .bearer_auth(v1)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    // Metrics are scoped to the caller's pairs.
    let scoped: Value = client
        .get(format!("http://{addr}/v1/metrics?window=2"))
        .bearer_auth(v1)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        scoped["pairs"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["BTC-USDC"]
    );
    assert_eq!(scoped["pairs"]["BTC-USDC"]["rows"], 3);
    assert!(
        scoped["pairs"]["BTC-USDC"]["window_digest"]
            .as_str()
            .unwrap()
            .len()
            == 64
    );
    let full: Value = client
        .get(format!("http://{addr}/v1/metrics?window=3"))
        .bearer_auth(v2)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(full["pairs"].as_object().unwrap().len(), 2);
    assert_eq!(full["ready"], true);

    // Pair scope: v1 may not subscribe to ETH.
    let (mut socket, _) = connect_async(request(&addr, v1)).await.unwrap();
    socket
        .send(Message::Text(
            json!({"v":1,"op":"subscribe","pairs":["ETH-USDC"],"history":2})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        ended(
            tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
        ),
        "out-of-scope pair must end the stream"
    );
    // History ceiling: v1 is capped at 2 even though the service holds 3.
    let mut socket = connect_retry(&addr, v1).await;
    socket
        .send(Message::Text(
            json!({"v":1,"op":"subscribe","pairs":["BTC-USDC"],"history":3})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert!(
        ended(
            tokio::time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
        ),
        "history above the ceiling must end the stream"
    );
    // Allowed subscription works and receives a snapshot trimmed to the requested history.
    let mut good = connect_retry(&addr, v1).await;
    good.send(Message::Text(
        json!({"v":1,"op":"subscribe","pairs":["BTC-USDC"],"history":2})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let frame: Value =
        serde_json::from_str(good.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(frame["type"], "snapshot");
    assert_eq!(frame["rows"].as_array().unwrap().len(), 2);
    // Stream cap: v1 allows exactly one concurrent stream; the second is refused with 503.
    let refused = connect_async(request(&addr, v1)).await;
    assert!(refused.is_err(), "second v1 stream must be refused");
    // v2 still has capacity: consumers do not starve each other.
    let (_other, _) = connect_async(request(&addr, v2)).await.unwrap();
    drop(good);
    let mut refused_scope = 0;
    for _ in 0..50 {
        refused_scope = health_counter(&client, &addr, v2, "client_scope_refused").await;
        if refused_scope >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        refused_scope, 2,
        "both out-of-scope subscriptions were counted by cause"
    );
    assert_eq!(
        health_counter(&client, &addr, v2, "client_stream_cap_refused").await,
        1
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn write_deadline_scales_with_frame_size_and_is_capped() {
    assert_eq!(server::write_deadline(0), Duration::from_secs(2));
    assert!(server::write_deadline(300) < Duration::from_millis(2_001));
    let snapshot = server::write_deadline(1_300_000);
    assert!(
        snapshot > Duration::from_secs(4) && snapshot < Duration::from_secs(5),
        "{snapshot:?}"
    );
    assert_eq!(
        server::write_deadline(usize::MAX / 4),
        Duration::from_secs(20)
    );
}

fn private_file(dir: &std::path::Path, name: &str, body: &str, mode: u32) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    path
}

#[test]
fn consumers_file_is_validated_like_a_secret_and_bounded_by_the_universe() {
    let dir = std::env::temp_dir().join(format!("okx-candle-hardening-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let universe = private_file(
        &dir,
        "universe.json",
        r#"{"pairs":["BTC-USDC","ETH-USDC"]}"#,
        0o644,
    );
    let legacy = private_file(
        &dir,
        "token",
        "legacy_token_0123456789abcdef0123456789abcdef\n",
        0o600,
    );
    let good = json!({"consumers":[
        {"id":"v1","token":"a1_token_0123456789abcdef0123456789abcdef","pairs":["BTC-USDC"],"max_history":50,"max_streams":4},
        {"id":"v2","token":"a2_token_0123456789abcdef0123456789abcdef","pairs":["BTC-USDC","ETH-USDC"],"max_history":100,"max_streams":8}
    ]});
    let write =
        |name: &str, value: &Value, mode: u32| private_file(&dir, name, &value.to_string(), mode);
    let args = |consumers: Option<&std::path::Path>, token: Option<&std::path::Path>| {
        let mut argv = vec![
            "okx-candle-service".to_string(),
            "--universe".into(),
            universe.display().to_string(),
            "--history".into(),
            "100".into(),
        ];
        if let Some(path) = consumers {
            argv.extend(["--consumers-file".to_string(), path.display().to_string()]);
        }
        if let Some(path) = token {
            argv.extend(["--token-file".to_string(), path.display().to_string()]);
        }
        Args::try_parse_from(argv).unwrap()
    };
    let ok = write("ok.json", &good, 0o600);
    let (pairs, auth) = args(Some(&ok), Some(&legacy)).validate().unwrap();
    assert_eq!(pairs.len(), 2);
    assert_eq!(
        auth.consumers().len(),
        3,
        "legacy token plus two scoped consumers"
    );
    assert!(
        auth.consumers()
            .iter()
            .any(|c| c.id == "v1" && !c.allows_pair("ETH-USDC") && c.max_history == 50)
    );
    assert!(
        args(Some(&ok), None).validate().is_ok(),
        "scoped tokens alone are enough"
    );
    assert!(
        args(None, None).validate().is_err(),
        "at least one credential source is required"
    );
    let reject = |name: &str, mutate: &dyn Fn(&mut Value), mode: u32| {
        let mut value = good.clone();
        mutate(&mut value);
        let path = write(name, &value, mode);
        let error = args(Some(&path), None).validate().unwrap_err().to_string();
        assert!(
            !error.contains("0123456789abcdef"),
            "validation errors must not echo tokens: {error}"
        );
    };
    reject("perm.json", &|_| {}, 0o644);
    reject(
        "short.json",
        &|v| v["consumers"][0]["token"] = json!("short"),
        0o600,
    );
    reject(
        "dup.json",
        &|v| v["consumers"][1]["token"] = v["consumers"][0]["token"].clone(),
        0o600,
    );
    reject(
        "dupid.json",
        &|v| v["consumers"][1]["id"] = json!("v1"),
        0o600,
    );
    reject(
        "pair.json",
        &|v| v["consumers"][0]["pairs"] = json!(["SOL-USDC"]),
        0o600,
    );
    reject(
        "hist.json",
        &|v| v["consumers"][0]["max_history"] = json!(101),
        0o600,
    );
    reject(
        "streams.json",
        &|v| v["consumers"][0]["max_streams"] = json!(0),
        0o600,
    );
    reject(
        "id.json",
        &|v| v["consumers"][0]["id"] = json!("Bad Id"),
        0o600,
    );
    reject(
        "extra.json",
        &|v| v["consumers"][0]["admin"] = json!(true),
        0o600,
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn status_file_is_private_atomic_and_carries_the_metrics_view() {
    let dir = std::env::temp_dir().join(format!("okx-candle-status-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("status.json");
    okx_candle_service::status::validate(&path).unwrap();
    assert!(okx_candle_service::status::validate(std::path::Path::new("relative.json")).is_err());
    assert!(okx_candle_service::status::validate(&dir.join("missing-dir/status.json")).is_err());
    let state = State::new(&["BTC-USDC".into()], 2);
    let stop = CancellationToken::new();
    let task = tokio::spawn(okx_candle_service::status::run(
        state.clone(),
        path.clone(),
        stop.clone(),
    ));
    // First tick fires immediately.
    let mut value = Value::Null;
    for _ in 0..100 {
        if let Ok(bytes) = std::fs::read(&path) {
            value = serde_json::from_slice(&bytes).unwrap();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(value["schema"], "okx-candle-service.metrics.v1");
    assert_eq!(value["pairs"]["BTC-USDC"]["ready"], false);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let leftovers: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        leftovers.len(),
        1,
        "temporary files are cleaned up: {leftovers:?}"
    );
    stop.cancel();
    task.await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
