use crate::{
    history::{HistoryPager, PageRequest},
    model::{Candle, now, parse_row},
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
const WS: &str = "wss://ws.okx.com:8443/ws/v5/business";

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
        let (state, pairs, pace, stop) = (
            state.clone(),
            chunk.to_vec(),
            connect_pace.clone(),
            stop.clone(),
        );
        tasks.spawn(async move {
            let mut backoff = 1;
            loop {
                let started = Instant::now();
                let outcome = tokio::select! { _ = stop.cancelled() => break, r = connection(state.clone(), &pairs, pace.clone()) => r };
                let reason = if let Err(error) = outcome { eprintln!("public candle connection: {error:#}"); "upstream_disconnected" } else { "upstream_closed" };
                state.lock().await.disconnected(&pairs, reason);
                if started.elapsed() > Duration::from_secs(60) { backoff = 1; }
                tokio::select! { _ = stop.cancelled() => break, _ = sleep(Duration::from_secs(backoff)) => {} }
                backoff = (backoff * 2).min(30);
            }
        });
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

async fn connection(state: Shared, pairs: &[String], pace: Pace) -> Result<()> {
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
    session(state, pairs, socket, SessionTiming::production()).await
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

async fn session<S>(
    state: Shared,
    pairs: &[String],
    mut socket: WebSocketStream<S>,
    timing: SessionTiming,
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
    let mut next_ping = Instant::now() + timing.ping;
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
                    acknowledged.insert(pair.to_string()); state.lock().await.acknowledge(pair); continue;
                }
                ensure!(frame["arg"]["channel"] == "candle1m", "wrong candle channel");
                let pair = frame["arg"]["instId"].as_str().context("missing candle pair")?;
                // OKX may interleave requested-pair data ahead of its subscription ACK.
                // Buffer it under the existing bound, but only acknowledge() enables seeding.
                ensure!(catalog.contains(pair), "candle received for unrequested pair");
                let rows = frame["data"].as_array().context("missing candle data")?;
                ensure!(!rows.is_empty() && rows.len() <= 300, "invalid candle batch size");
                let parsed: Result<Vec<_>> = rows.iter().map(|row| parse_row(row, received_at)).collect();
                let mut parsed: Vec<_> = match parsed {
                    Ok(rows) => rows.into_iter().flatten().collect(),
                    Err(error) => {
                        eprintln!("invalid public candle {pair}: {error:#}");
                        state.lock().await.reject_pair_data(pair);
                        continue;
                    }
                };
                parsed.sort_by_key(|r| r.open_time);
                let mut state = state.lock().await;
                state.count("upstream_frames");
                if !acknowledged.contains(pair) { state.count("buffered_before_ack"); }
                for row in parsed { state.ingest(pair, row, received_at); }
                // All raw rows passed the strict parser, including forming OHLC.
                // The forming head is liveness evidence, never confirmed history.
                if let Some(head)=rows.iter().max_by_key(|r|r[0].as_str().unwrap().parse::<u64>().unwrap()) {
                    state.observe_live_head(pair,head[0].as_str().unwrap().parse::<u64>().unwrap()/1000,head[8]=="1",received_at);
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
        if let Some(row) = parse_row(row, received_at)?
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

async fn repair_loop(state: Shared, client: reqwest::Client, stop: CancellationToken) {
    let pace = Pace::new(Duration::from_millis(210));
    let mut jobs = tokio::task::JoinSet::new();
    let mut tick = interval(Duration::from_secs(2));
    let mut retry_after = std::collections::BTreeMap::new();
    loop {
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tick.tick() => {
                let mut s = state.lock().await;
                let stale: Vec<_> = s.pairs.iter().filter(|(_,p)| p.ready && p.rows.back().is_none_or(|r| now() - (r.open_time + 60) as f64 > 90.0)).map(|(name,_)|name.clone()).collect();
                for pair in stale { s.invalidate(&pair, "stale_confirmed_history"); }
            }
            Some(result) = jobs.join_next(), if !jobs.is_empty() => {
                match result {
                    Ok(pair) => { retry_after.insert(pair, Instant::now()+Duration::from_secs(10)); },
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
            retry_after.insert(pair.clone(), Instant::now() + Duration::from_secs(10));
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
                match result {
                    Ok((rows, at)) => s.apply_seed(&pair, generation, rows, at),
                    Err(error) => {
                        eprintln!("public REST seed {pair}: {error:#}");
                        if s.pairs[&pair].generation == generation && !s.pairs[&pair].ready {
                            s.pairs.get_mut(&pair).unwrap().seeding = false;
                            s.invalidate(&pair, "rest_seed_failed");
                        }
                        s.count("seed_failures");
                    }
                }
                pair
            });
        }
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
}

#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
