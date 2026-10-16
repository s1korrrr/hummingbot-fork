use crate::{
    history::{HistoryPager, PageRequest, StaleRestPage},
    model::{Candle, Parsed, now, parse_row_tolerant},
    state::Shared,
};
use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex,
    time::{Instant, interval, sleep, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const REST: &str = "https://www.okx.com/api/v5/market/candles";
const HISTORY_REST: &str = "https://www.okx.com/api/v5/market/history-candles";
const WS: &str = "wss://ws.okx.com/ws/v5/business";

/// Shared across every request/connection task in this process. No burst credit.
#[derive(Clone)]
struct Pace {
    next: Arc<Mutex<Instant>>,
    spacing: Duration,
}
impl Pace {
    fn new(spacing: Duration) -> Self {
        Self {
            next: Arc::new(Mutex::new(Instant::now())),
            spacing,
        }
    }
    async fn acquire(&self) {
        let mut next = self.next.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = Instant::now() + self.spacing;
    }
}

/// Independent public WebSocket lanes per pair group. Rows from every lane are merged and
/// deduplicated by `State::ingest`, and a pair stays online while any lane is acknowledged.
pub const LANES: u8 = 2;

/// Full-jitter exponential reconnect delay: `min(30, 2^attempt)` seconds scaled into
/// `[0.5, 1.0)` by `unit` (a uniform draw in `[0, 1)`), so lanes never reconnect in lockstep.
pub fn reconnect_delay(attempt: u32, unit: f64) -> Duration {
    let base = f64::from(2u32.saturating_pow(attempt.min(5))).min(30.0);
    Duration::from_secs_f64(base * (0.5 + 0.5 * unit.clamp(0.0, 0.999_999)))
}

/// Seed retry delay: exponential per consecutive failure with jitter. A stale REST edge
/// retries quickly (it usually clears within seconds); other failures start slower.
pub fn seed_retry_delay(failures: u32, stale_page: bool, unit: f64) -> Duration {
    let base = if stale_page { 3.0 } else { 10.0 };
    let grown =
        (base * f64::from(2u32.saturating_pow(failures.saturating_sub(1).min(4)))).min(60.0);
    Duration::from_secs_f64(grown * (0.75 + 0.5 * unit.clamp(0.0, 0.999_999)))
}

/// Uniform draw in `[0, 1)` without an extra dependency.
pub fn unit_random() -> f64 {
    let bytes = *uuid::Uuid::new_v4().as_bytes();
    let n = u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes")) >> 11;
    n as f64 / (1u64 << 53) as f64
}

pub async fn run(state: Shared, pairs: Vec<String>, stop: CancellationToken) -> Result<()> {
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(10))
        .build()?;
    let mut tasks = tokio::task::JoinSet::new();
    let connect_pace = Pace::new(Duration::from_millis(1050));
    for chunk in pairs.chunks(100) {
        for lane in 0..LANES {
            let (state, pairs, pace, stop) = (
                state.clone(),
                chunk.to_vec(),
                connect_pace.clone(),
                stop.clone(),
            );
            tasks.spawn(async move {
                // Lane n starts n*15 s after lane 0 so the two sockets never share a fate window.
                let stagger = Duration::from_secs(15 * u64::from(lane));
                tokio::select! { _ = stop.cancelled() => return, _ = sleep(stagger) => {} }
                let mut attempt = 0u32;
                loop {
                    let started = Instant::now();
                    let outcome = tokio::select! { _ = stop.cancelled() => break, r = connection(state.clone(), &pairs, pace.clone(), lane) => r };
                    let reason = if let Err(error) = outcome { eprintln!("public candle connection lane={lane}: {error:#}"); "upstream_disconnected" } else { "upstream_closed" };
                    state.lock().await.lane_disconnected(&pairs, lane, reason);
                    if started.elapsed() > Duration::from_secs(60) { attempt = 0; }
                    tokio::select! { _ = stop.cancelled() => break, _ = sleep(reconnect_delay(attempt, unit_random())) => {} }
                    attempt = attempt.saturating_add(1);
                }
            });
        }
    }
    tasks.spawn(repair_loop(state.clone(), client, stop.clone()));
    tokio::select! {
        _ = stop.cancelled() => {},
        result = tasks.join_next() => { if !stop.is_cancelled() { bail!("collector task stopped unexpectedly: {result:?}"); } }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    state.lock().await.disconnected(&pairs, "shutdown");
    Ok(())
}

async fn connection(state: Shared, pairs: &[String], pace: Pace, lane: u8) -> Result<()> {
    pace.acquire().await;
    state.lock().await.count("connection_attempts");
    let config = WebSocketConfig::default()
        .max_message_size(Some(1_048_576))
        .max_frame_size(Some(1_048_576));
    let (socket, _) = timeout(
        Duration::from_secs(15),
        connect_async_with_config(WS, Some(config), false),
    )
    .await??;
    session_lane(state, pairs, socket, SessionTiming::production(), lane).await
}

// Private injection boundary for offline protocol tests. Production callers always
// use fixed endpoint/TLS setup above and these fixed timing values.
#[derive(Clone, Copy)]
struct SessionTiming {
    acknowledgement: Duration,
    ping: Duration,
    pong: Duration,
    check: Duration,
}
impl SessionTiming {
    fn production() -> Self {
        Self {
            acknowledgement: Duration::from_secs(20),
            ping: Duration::from_secs(20),
            pong: Duration::from_secs(10),
            check: Duration::from_millis(250),
        }
    }
}

#[cfg(test)]
async fn session<S>(
    state: Shared,
    pairs: &[String],
    socket: WebSocketStream<S>,
    timing: SessionTiming,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    session_lane(state, pairs, socket, timing, 0).await
}

#[cfg(test)]
#[test]
fn candle_collector_uses_okx_default_websocket_port() {
    assert_eq!(WS, "wss://ws.okx.com/ws/v5/business");
}

async fn session_lane<S>(
    state: Shared,
    pairs: &[String],
    mut socket: WebSocketStream<S>,
    timing: SessionTiming,
    lane: u8,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let args: Vec<_> = pairs
        .iter()
        .map(|p| json!({"channel":"candle1m","instId":p}))
        .collect();
    timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(
            json!({"op":"subscribe","args":args}).to_string().into(),
        )),
    )
    .await??;
    let catalog: BTreeSet<_> = pairs.iter().cloned().collect();
    let mut acknowledged = BTreeSet::new();
    let ack_deadline = Instant::now() + timing.acknowledgement;
    let mut next_ping = Instant::now() + timing.ping + timing.ping * u32::from(lane) / 3;
    let mut pong_deadline: Option<Instant> = None;
    let mut timer = interval(timing.check);
    loop {
        tokio::select! {
            biased;
            _ = timer.tick() => {
                let at = Instant::now();
                ensure!(acknowledged.len() == catalog.len() || at < ack_deadline, "subscription acknowledgement timeout");
                ensure!(pong_deadline.is_none_or(|deadline| at < deadline), "text pong timeout");
                if at >= next_ping && pong_deadline.is_none() {
                    timeout(Duration::from_secs(5), socket.send(Message::Text("ping".into()))).await??;
                    pong_deadline = Some(Instant::now() + timing.pong);
                    next_ping = Instant::now() + timing.ping;
                    state.lock().await.count("text_pings");
                }
            }
            message = socket.next() => {
                let message = message.context("upstream EOF")??;
                let received_at = now();
                let text = match message {
                    Message::Text(s) => s,
                    Message::Ping(v) => { timeout(Duration::from_secs(5), socket.send(Message::Pong(v))).await??; continue; },
                    Message::Pong(_) => continue,
                    Message::Close(_) => bail!("upstream close"),
                    _ => bail!("unexpected binary upstream frame"),
                };
                if text == "pong" { ensure!(pong_deadline.is_some(), "unsolicited text pong"); pong_deadline = None; state.lock().await.count("text_pongs"); continue; }
                let frame: Value = serde_json::from_str(&text)?;
                if frame.get("event").is_some() {
                    ensure!(frame["event"] == "subscribe", "unexpected upstream event: {}", frame["event"]);
                    ensure!(frame["arg"]["channel"] == "candle1m", "wrong acknowledged channel");
                    let pair = frame["arg"]["instId"].as_str().context("missing ack pair")?;
                    ensure!(catalog.contains(pair), "unexpected ack pair");
                    acknowledged.insert(pair.to_string()); state.lock().await.acknowledge_lane(pair, lane); continue;
                }
                ensure!(frame["arg"]["channel"] == "candle1m", "wrong candle channel");
                let pair = frame["arg"]["instId"].as_str().context("missing candle pair")?;
                // OKX may interleave requested-pair data ahead of its subscription ACK.
                // Buffer it under the existing bound, but only acknowledge() enables seeding.
                ensure!(catalog.contains(pair), "candle received for unrequested pair");
                let rows = frame["data"].as_array().context("missing candle data")?;
                ensure!(!rows.is_empty() && rows.len() <= 300, "invalid candle batch size");
                let mut confirmed = Vec::new();
                let mut deferred = Vec::new();
                let mut ignored = Vec::new();
                let mut head: Option<(u64, bool)> = None;
                let mut fatal = None;
                for row in rows {
                    match parse_row_tolerant(row, received_at) {
                        Ok(Parsed::Confirmed(candle)) => {
                            if head.is_none_or(|(open, _)| candle.open_time >= open) { head = Some((candle.open_time, true)); }
                            confirmed.push(candle);
                        }
                        Ok(Parsed::Deferred(candle)) => deferred.push(candle),
                        Ok(Parsed::Forming(open_time)) => {
                            if head.is_none_or(|(open, _)| open_time > open) { head = Some((open_time, false)); }
                        }
                        Ok(Parsed::Ignored(reason)) => ignored.push(reason),
                        Err(error) => { fatal = Some(error); break; }
                    }
                }
                if let Some(error) = fatal {
                    eprintln!("invalid public candle {pair}: {error:#}");
                    state.lock().await.reject_pair_data(pair);
                    continue;
                }
                confirmed.sort_by_key(|r| r.open_time);
                let mut state = state.lock().await;
                state.count("upstream_frames");
                if !acknowledged.contains(pair) { state.count("buffered_before_ack"); }
                for reason in ignored { state.note_rejected(pair, reason); }
                for candle in deferred { state.defer(pair, candle, received_at); }
                for row in confirmed { state.ingest(pair, row, received_at); }
                // The newest plausible bar, forming or confirmed, is liveness evidence only;
                // bars ahead of the local clock never become a head.
                if let Some((open_time, confirmed)) = head {
                    state.observe_live_head(pair, open_time, confirmed, received_at);
                }
            }
        }
    }
}

async fn fetch_page(
    client: &reqwest::Client,
    pair: &str,
    request: PageRequest,
) -> Result<(Vec<Candle>, f64)> {
    let mut query = vec![
        ("instId", pair.to_string()),
        ("bar", "1m".into()),
        ("limit", request.limit.to_string()),
    ];
    if let Some(after) = request.after_ms {
        query.push(("after", after.to_string()));
    }
    let response = client
        .get(if request.after_ms.is_some() {
            HISTORY_REST
        } else {
            REST
        })
        .query(&query)
        .send()
        .await?
        .error_for_status()?;
    ensure!(
        response.content_length().is_none_or(|n| n <= 262_144),
        "REST body too large"
    );
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(part) = timeout(Duration::from_secs(15), stream.next()).await? {
        let part = part?;
        ensure!(bytes.len() + part.len() <= 262_144, "REST body too large");
        bytes.extend_from_slice(&part);
    }
    let received_at = now();
    let value: Value = serde_json::from_slice(&bytes)?;
    ensure!(value["code"] == "0", "REST returned nonzero code");
    let rows = value["data"].as_array().context("missing REST data")?;
    ensure!(rows.len() <= 300, "REST returned too many rows");
    let mut parsed = std::collections::BTreeMap::new();
    for row in rows {
        // The forming bar, unconfirmed noise and confirmed bars not yet closed on the local
        // clock are skipped, never fatal: only confirmed, closed rows enter a seed.
        if let Parsed::Confirmed(row) = parse_row_tolerant(row, received_at)?
            && let Some(old) = parsed.insert(row.open_time, row.clone())
        {
            ensure!(old == row, "conflicting REST rows");
        }
    }
    Ok((parsed.into_values().collect(), received_at))
}

async fn fetch_history<F, Fut>(
    history: usize,
    base: Vec<Candle>,
    pace: &Pace,
    mut fetch: F,
) -> Result<(Vec<Candle>, f64)>
where
    F: FnMut(PageRequest) -> Fut,
    Fut: std::future::Future<Output = Result<(Vec<Candle>, f64)>>,
{
    let mut pager = HistoryPager::new(history, base);
    loop {
        let request = pager.request(now())?;
        pace.acquire().await;
        let (page, received_at) = fetch(request).await?;
        if let Some(rows) = pager.accept(page, received_at)? {
            return Ok((rows, received_at));
        }
    }
}

/// Result of one seed attempt, reported back to the repair loop for retry scheduling.
enum SeedOutcome {
    Seeded,
    Failed { stale_page: bool },
}

async fn repair_loop(state: Shared, client: reqwest::Client, stop: CancellationToken) {
    let pace = Pace::new(Duration::from_millis(210));
    let mut jobs: tokio::task::JoinSet<(String, SeedOutcome)> = tokio::task::JoinSet::new();
    let mut tick = interval(Duration::from_secs(1));
    let mut retry_after = std::collections::BTreeMap::new();
    let mut failures: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick.tick() => {
                let mut s = state.lock().await;
                // Reconcile confirmed bars only when the local clock reaches their close.
                s.release_deferred(now());
                let stale: Vec<_> = s.pairs.iter().filter(|(_,p)| p.ready && p.rows.back().is_none_or(|r| now() - (r.open_time + 60) as f64 > 90.0)).map(|(name,_)|name.clone()).collect();
                for pair in stale { s.invalidate(&pair, "stale_confirmed_history"); }
            }
            Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                match result {
                    Ok((pair, SeedOutcome::Seeded)) => { failures.remove(&pair); retry_after.remove(&pair); },
                    Ok((pair, SeedOutcome::Failed { stale_page })) => {
                        let count = failures.entry(pair.clone()).or_default();
                        *count += 1;
                        retry_after.insert(pair, Instant::now() + seed_retry_delay(*count, stale_page, unit_random()));
                    },
                    Err(error) => { eprintln!("seed task failed: {error}"); stop.cancel(); break; }
                }
            }
        }
        while jobs.len() < 4 {
            let mut s = state.lock().await;
            let candidate = s
                .pairs
                .iter()
                .find(|(name, p)| {
                    p.online
                        && !p.ready
                        && !p.seeding
                        && retry_after
                            .get(*name)
                            .is_none_or(|at| Instant::now() >= *at)
                })
                .map(|(name, p)| (name.clone(), p.generation));
            let Some((pair, generation)) = candidate else {
                break;
            };
            s.pairs.get_mut(&pair).unwrap().seeding = true;
            let history = s.capacity;
            let base = s.pairs[&pair]
                .seed_base
                .iter()
                .map(|r| r.as_ref().clone())
                .collect();
            drop(s);
            // Minimum spacing even if the job result is lost; completion replaces it.
            retry_after.insert(pair.clone(), Instant::now() + Duration::from_secs(3));
            let (state, client, pace) = (state.clone(), client.clone(), pace.clone());
            jobs.spawn(async move {
                let result = fetch_history(history, base, &pace, |request| {
                    let state = state.clone();
                    let client = client.clone();
                    let pair = pair.clone();
                    async move {
                        state.lock().await.count("rest_requests");
                        fetch_page(&client, &pair, request).await
                    }
                })
                .await;
                let mut s = state.lock().await;
                let outcome = match result {
                    Ok((rows, at)) => {
                        if s.apply_seed(&pair, generation, rows, at) {
                            SeedOutcome::Seeded
                        } else {
                            SeedOutcome::Failed { stale_page: false }
                        }
                    }
                    Err(error) => {
                        let stale_page = error.downcast_ref::<StaleRestPage>().is_some();
                        eprintln!("public REST seed {pair}: {error:#}");
                        if s.pairs[&pair].generation == generation && !s.pairs[&pair].ready {
                            s.pairs.get_mut(&pair).unwrap().seeding = false;
                            s.invalidate(&pair, "rest_seed_failed");
                        }
                        s.count("seed_failures");
                        if stale_page {
                            s.count("seed_stale_page");
                        }
                        SeedOutcome::Failed { stale_page }
                    }
                };
                (pair, outcome)
            });
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
}

#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
