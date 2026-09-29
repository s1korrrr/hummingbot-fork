use crate::state::{Event, Shared};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Semaphore, broadcast},
    time::{interval, timeout},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct App {
    pub state: Shared,
    pub token: Arc<String>,
    pub clients: Arc<Semaphore>,
    pub stop: CancellationToken,
    pub market_picture: Option<crate::market_picture::SharedProjection>,
    pub market_picture_clients: Arc<Semaphore>,
}
pub fn router(
    state: Shared,
    token: String,
    stop: CancellationToken,
    market_picture: Option<crate::market_picture::SharedProjection>,
) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v2/market-picture/history", get(market_picture_history))
        .route("/v2/market-picture/updates", get(market_picture_updates))
        .route("/v1/stream", get(upgrade))
        .with_state(App {
            state,
            token: Arc::new(token),
            clients: Arc::new(Semaphore::new(32)),
            stop,
            market_picture,
            market_picture_clients: Arc::new(Semaphore::new(16)),
        })
}
fn authorized(headers: &HeaderMap, token: &str) -> bool {
    let Some(value) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    if value.len() != token.len() {
        return false;
    }
    value
        .bytes()
        .zip(token.bytes())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
async fn health(State(app): State<App>, headers: HeaderMap) -> Response {
    if !authorized(&headers, &app.token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(app.state.lock().await.health()).into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketPictureQuery {
    pair: String,
    interval: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketPictureUpdatesQuery {
    pair: String,
    interval: String,
    epoch: Option<String>,
    sequence: Option<u64>,
}

async fn market_picture_updates(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<MarketPictureUpdatesQuery>,
) -> Response {
    if !authorized(&headers, &app.token) || headers.contains_key("origin") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(interval) = crate::market_picture::Interval::parse(&query.interval) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !crate::model::valid_pair(&query.pair)
        || query.epoch.as_deref().is_some_and(|epoch| epoch.len() > 64)
        || query.epoch.is_some() != query.sequence.is_some()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(_permit) = app.market_picture_clients.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let now_ms = (crate::model::now() * 1000.0).round() as u64;
    let updates = if let Some(projection) = app.market_picture {
        let projection = projection.lock().await;
        match projection.updates(
            &query.pair,
            interval,
            query.epoch.as_deref(),
            query.sequence,
            now_ms,
        ) {
            Ok(value) => value,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        }
    } else {
        let mut response = crate::market_picture::Projection::unavailable(
            &query.pair,
            interval,
            now_ms,
            "projection_disabled",
        );
        response.schema_version = "market-picture.source-updates.v1".into();
        crate::market_picture::UpdatesResponse {
            mode: "full".into(),
            from_sequence: None,
            history: response,
            upserts: Vec::new(),
            deleted_open_time_ms: Vec::new(),
        }
    };
    match serde_json::to_vec(&updates) {
        Ok(bytes)
            if bytes.len()
                <= if updates.mode == "delta" {
                    crate::market_picture::MAX_UPDATE_RESPONSE_BYTES
                } else {
                    crate::market_picture::MAX_RESPONSE_BYTES
                } =>
        {
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                bytes,
            )
                .into_response()
        }
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn market_picture_history(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<MarketPictureQuery>,
) -> Response {
    if !authorized(&headers, &app.token) || headers.contains_key("origin") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(interval) = crate::market_picture::Interval::parse(&query.interval) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !crate::model::valid_pair(&query.pair) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(_permit) = app.market_picture_clients.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let now_ms = (crate::model::now() * 1000.0).round() as u64;
    let response = if let Some(projection) = app.market_picture {
        let projection = projection.lock().await;
        match projection.response(&query.pair, interval, now_ms) {
            Ok(value) => value,
            Err(_) => return StatusCode::NOT_FOUND.into_response(),
        }
    } else {
        crate::market_picture::Projection::unavailable(
            &query.pair,
            interval,
            now_ms,
            "projection_disabled",
        )
    };
    match serde_json::to_vec(&response) {
        Ok(bytes) if bytes.len() <= crate::market_picture::MAX_RESPONSE_BYTES => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
async fn upgrade(State(app): State<App>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    if !authorized(&headers, &app.token) || headers.contains_key("origin") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(permit) = app.clients.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    ws.max_message_size(131072)
        .max_frame_size(131072)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            if let Err(error) = serve(socket, app.clone()).await {
                eprintln!("local stream closed: {error:#}");
                app.state.lock().await.count("client_disconnect_errors");
            }
        })
        .into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Subscription {
    v: u8,
    op: String,
    pairs: Vec<String>,
    history: usize,
}
async fn send(socket: &mut WebSocket, value: Value) -> Result<()> {
    timeout(
        Duration::from_secs(2),
        socket.send(Message::Text(value.to_string().into())),
    )
    .await??;
    Ok(())
}
fn filter(event: Event, sent: &mut BTreeMap<String, u64>) -> Option<Event> {
    let frame = event.metadata();
    let pair = frame["pair"].as_str()?;
    let last = sent.get_mut(pair)?;
    let sequence = frame["sequence"].as_u64()?;
    if sequence <= *last {
        return None;
    }
    *last = sequence;
    Some(event)
}

async fn serve(mut socket: WebSocket, app: App) -> Result<()> {
    let message = timeout(Duration::from_secs(5), socket.recv())
        .await?
        .context("no subscription")??;
    let Message::Text(text) = message else {
        anyhow::bail!("subscription must be text");
    };
    let sub: Subscription = serde_json::from_str(&text)?;
    ensure!(
        sub.v == 1 && sub.op == "subscribe" && !sub.pairs.is_empty(),
        "invalid subscription"
    );
    let mut sent = BTreeMap::new();
    let mut receiver = {
        let s = app.state.lock().await;
        ensure!(
            sub.history >= 2 && sub.history <= s.capacity,
            "history outside capacity"
        );
        ensure!(
            sub.pairs.len() <= s.pairs.len()
                && sub.pairs.iter().collect::<BTreeSet<_>>().len() == sub.pairs.len(),
            "duplicate or oversized subscription"
        );
        ensure!(
            sub.pairs.iter().all(|p| s.pairs.contains_key(p)),
            "unknown pair"
        );
        s.events.subscribe()
    };
    // Materialize one pair at a time instead of duplicating the entire universe's
    // full history as JSON. Receiver registration still precedes every snapshot.
    for pair in &sub.pairs {
        let frame = {
            let s = app.state.lock().await;
            sent.insert(pair.clone(), s.pairs[pair].sequence);
            s.frame(pair, sub.history)
        };
        send(&mut socket, frame).await?;
    }
    let mut heartbeats = interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            biased;
            _ = app.stop.cancelled() => { let _ = timeout(Duration::from_secs(2), socket.send(Message::Close(None))).await; break; }
            _ = heartbeats.tick() => {
                // Producer publishes while holding this same lock. Drain + sample are atomic,
                // so a heartbeat can never announce a sequence before its delta/snapshot.
                let (queued, heartbeat) = {
                    let s = app.state.lock().await; let mut queued = Vec::new();
                    loop {
                        match receiver.try_recv() {
                            Ok(frame) => if let Some(frame) = filter(frame,&mut sent) { queued.push(frame); },
                            Err(broadcast::error::TryRecvError::Empty) => break,
                            Err(error) => return Err(error.into()),
                        }
                    }
                    let mut heartbeat = s.envelope("heartbeat");
                    heartbeat["states"] = json!(sent.iter().map(|(pair,seq)| (pair,json!({"ready":s.pairs[pair].ready,"sequence":seq}))).collect::<BTreeMap<_,_>>());
                    (queued, heartbeat)
                };
                for event in queued { send(&mut socket,event.into_frame(sub.history)).await?; }
                send(&mut socket,heartbeat).await?;
            }
            frame = receiver.recv() => {
                if let Some(event) = filter(frame?,&mut sent) { send(&mut socket,event.into_frame(sub.history)).await?; }
            }
            message = socket.recv() => {
                match message {
                    Some(Ok(Message::Ping(v))) => { timeout(Duration::from_secs(2),socket.send(Message::Pong(v))).await??; },
                    Some(Ok(Message::Pong(_))) => {},
                    None | Some(Ok(Message::Close(_))) => break,
                    _ => anyhow::bail!("unexpected client message"),
                }
            }
        }
    }
    Ok(())
}
