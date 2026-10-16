//! Scoped, read-only observation transport. It never activates a source.
use crate::{auth::Consumer, server::App};
use axum::{
    Json,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::SinkExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::broadcast,
    time::{interval, timeout},
};

const MAX_OUTPUT_BYTES: usize = 2 * 1024 * 1024;

fn authenticate(app: &App, headers: &HeaderMap) -> Option<Arc<Consumer>> {
    if headers.contains_key("origin") {
        return None;
    }
    app.auth.authenticate(headers)
}

fn snapshot(app: &App, consumer: &Consumer) -> anyhow::Result<Value> {
    let mut value = match &app.observations {
        Some(capture) => capture.snapshot()?,
        None => json!({"enabled":false,"running":false,"instrument_ids":[],"channels":[]}),
    };
    if let Some(instruments) = value
        .get_mut("instrument_ids")
        .and_then(Value::as_array_mut)
    {
        instruments.retain(|item| {
            item.as_str()
                .and_then(|id| id.strip_prefix("okx:spot:"))
                .is_some_and(|pair| consumer.allows_pair(pair))
        });
    }
    if let Some(streams) = value.get_mut("streams").and_then(Value::as_object_mut) {
        streams.retain(|_, progress| {
            progress
                .get("instrument_id")
                .and_then(Value::as_str)
                .and_then(|id| id.strip_prefix("okx:spot:"))
                .is_some_and(|pair| consumer.allows_pair(pair))
        });
    }
    // A restricted reader cannot inspect another instrument's rejection details
    // or use global journal counters to infer its activity.
    if consumer.pairs.is_some() {
        for field in [
            "last_error",
            "journal_cursor",
            "last_update_sequence",
            "last_available_at_ms",
        ] {
            value
                .as_object_mut()
                .expect("object snapshot")
                .remove(field);
        }
    }
    value["schema_version"] = json!("okx.observations.health.v1");
    value["capital_authority"] = json!(false);
    value["source_qualification"] = json!("not_independently_qualified");
    value["replay_class"] = json!("live_updates_only");
    Ok(value)
}

pub async fn health(State(app): State<App>, headers: HeaderMap) -> Response {
    let Some(consumer) = authenticate(&app, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match snapshot(&app, &consumer) {
        Ok(value) => Json(value).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

pub async fn capabilities(State(app): State<App>, headers: HeaderMap) -> Response {
    let Some(consumer) = authenticate(&app, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match snapshot(&app, &consumer) {
        Ok(value) => Json(json!({
            "schema_version":"okx.observations.capabilities.v1", "enabled":value["enabled"],
            "instrument_ids":value["instrument_ids"],"source_profile":value["source_profile"],
            "channels":value["channels"],"individual_trade_channel":"trades-all",
            "bbo_channel":"bbo-tbt","bbo_depth_levels":1,"passive_queue_qualified":false,
            "source_qualification":"not_independently_qualified","capital_authority":false,
            "max_output_bytes":MAX_OUTPUT_BYTES,"resume":"reset_required", "history":"sealed_files_only",
        })).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamQuery {
    pair: String,
    epoch: Option<String>,
    sequence: Option<u64>,
}

pub async fn upgrade(
    State(app): State<App>,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(consumer) = authenticate(&app, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    if !crate::model::valid_pair(&query.pair) {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if !consumer.allows_pair(&query.pair) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if query.epoch.is_some() != query.sequence.is_some()
        || query.epoch.as_ref().is_some_and(|v| v.len() > 128)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    // Broadcast retention is not a recoverable tape. Never silently skip a cursor.
    if query.sequence.is_some() {
        return (
            StatusCode::GONE,
            Json(json!({"reason":"CURSOR_RESET_REQUIRED","replay_class":"live_updates_only"})),
        )
            .into_response();
    }
    let Some(capture) = &app.observations else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let instrument_id = format!("okx:spot:{}", query.pair);
    if !capture.configured_pairs().contains(&instrument_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(global_permit) = app.observation_clients.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let Some(scope) = app.observation_scopes.get(&consumer.id) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(scope_permit) = scope.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let receiver = capture.subscribe();
    let initial = match snapshot(&app, &consumer) {
        Ok(mut value) => {
            value["instrument_ids"] = json!([instrument_id]);
            value["type"] = json!("snapshot");
            value
        }
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    ws.max_message_size(1024)
        .max_frame_size(1024)
        .on_upgrade(move |socket| async move {
            let (_global, _scope) = (global_permit, scope_permit);
            if let Err(error) = stream(socket, app, instrument_id, initial, receiver).await {
                eprintln!("observation client disconnected: {error}");
            }
        })
}

async fn send(socket: &mut WebSocket, value: &Value) -> anyhow::Result<()> {
    let raw = serde_json::to_string(value)?;
    anyhow::ensure!(
        raw.len() <= MAX_OUTPUT_BYTES,
        "observation output exceeds byte bound"
    );
    timeout(
        Duration::from_secs(2),
        socket.send(Message::Text(raw.into())),
    )
    .await??;
    Ok(())
}

async fn stream(
    mut socket: WebSocket,
    app: App,
    instrument: String,
    initial: Value,
    mut receiver: broadcast::Receiver<crate::observation_capture::ObservationUpdate>,
) -> anyhow::Result<()> {
    // The receiver subscribed before the health snapshot. That snapshot carries
    // no event data and its high-water mark cannot suppress queued updates.
    let mut sent = 0;
    send(&mut socket, &initial).await?;
    let mut heartbeat = interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = app.stop.cancelled() => break,
            _ = heartbeat.tick() => send(&mut socket, &json!({"type":"heartbeat","last_delivered_sequence":sent,"freshness_renewed":false})).await?,
            item = receiver.recv() => match item {
                Ok(update) => {
                    if update.sequence <= sent { continue; }
                    // Connection-wide loss affects this reader, but its raw
                    // subscription/rejection payload can name other instruments.
                    if update.instrument_id.is_none() && matches!(update.record_kind.as_str(), "disconnect" | "gap" | "queue_rejected" | "source_switch") {
                        sent = update.sequence;
                        let reason = match update.record_kind.as_str() {
                            "gap" => "SOURCE_GAP", "queue_rejected" => "QUEUE_REJECTED",
                            "source_switch" => "SOURCE_EPOCH_CHANGED", _ => "SOURCE_DISCONNECTED",
                        };
                        send(&mut socket, &json!({"type":"source_status","reason":reason,
                            "record_kind":update.record_kind,"sequence":update.sequence,"epoch":update.epoch,
                            "instrument_id":instrument,"channel":update.channel,"quality":"unqualified",
                            "durable_cursor":update.durable_cursor,"available_at_ms":update.available_at_ms})).await?;
                        continue;
                    }
                    if update.instrument_id.is_none() && update.record_kind == "timer"
                        && matches!(update.channel.as_deref(), Some("trades-all" | "bbo-tbt")) {
                        // A durable channel timer advances evidence availability
                        // in a quiet interval. It never refreshes a quote or grants
                        // independent source/coverage qualification.
                        sent = update.sequence;
                        let mut coverage = serde_json::Map::new();
                        if let Some(value) = update.payload.get("coverage_by_instrument")
                            .and_then(|rows| rows.get(&instrument)) {
                            coverage.insert(instrument.clone(), value.clone());
                        }
                        send(&mut socket, &json!({"schema_version":"okx.observations.update.v1",
                            "record_kind":"timer","sequence":update.sequence,"epoch":update.epoch,
                            "instrument_id":instrument,"channel":update.channel,"quality":"unqualified",
                            "reason_codes":[],"durable_cursor":update.durable_cursor,
                            "available_at_ms":update.available_at_ms,"accepted_at_ms":update.accepted_at_ms,
                            "durable_acknowledged_at_ms":update.durable_acknowledged_at_ms,
                            "computed_at_ms":update.computed_at_ms,"published_at_ms":update.published_at_ms,
                            "payload":{"schema_version":"okx.observations.timer.v1","event":"timer",
                                "projection":"pair_scoped_channel_timer","instrument_id":instrument,
                                "channel":update.channel,"available_at_ms":update.available_at_ms,
                                "qualified_source":false,"coverage_by_instrument":coverage}})).await?;
                        continue;
                    }
                    if update.instrument_id.as_deref() != Some(instrument.as_str()) { continue; }
                    sent = update.sequence;
                    send(&mut socket, &serde_json::to_value(update)?).await?;
                },
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    send(&mut socket, &json!({"type":"reset","reason":"MISSED_UPDATES","replay_class":"live_updates_only"})).await?;
                    break;
                },
                Err(broadcast::error::RecvError::Closed) => break,
            },
            frame = socket.recv() => match frame {
                Some(Ok(Message::Ping(payload))) => { timeout(Duration::from_secs(2),socket.send(Message::Pong(payload))).await??; },
                Some(Ok(Message::Pong(_))) => {},
                None | Some(Ok(Message::Close(_))) => break,
                _ => anyhow::bail!("unexpected observation client message"),
            }
        }
    }
    timeout(Duration::from_secs(2), socket.close()).await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{auth::Auth, observation_capture::ObservationUpdate, state::State as CandleState};
    use futures_util::StreamExt;
    use std::collections::BTreeMap;
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn queued_event_is_not_discarded_by_snapshot_health_highwater() {
        let stop = CancellationToken::new();
        let app = App {
            state: CandleState::new(&["BTC-USDC".into()], 2),
            auth: Arc::new(Auth::single("synthetic".into(), 2, 1)),
            clients: Arc::new(Semaphore::new(1)),
            stop: stop.clone(),
            market_picture: None,
            market_picture_clients: Arc::new(Semaphore::new(1)),
            observations: None,
            observation_clients: Arc::new(Semaphore::new(1)),
            observation_scopes: Arc::new(BTreeMap::new()),
        };
        let (sender, receiver) = broadcast::channel(4);
        // Exact subscribe -> publish -> health snapshot interleaving. Snapshot
        // reports sequence 1 but contains none of this queued event's data.
        sender
            .send(ObservationUpdate {
                schema_version: "okx.observations.update.v1".into(),
                sequence: 1,
                epoch: "e".into(),
                record_kind: "trade".into(),
                channel: Some("trades-all".into()),
                instrument_id: Some("okx:spot:BTC-USDC".into()),
                available_at_ms: Some(100),
                accepted_at_ms: None,
                durable_acknowledged_at_ms: None,
                computed_at_ms: None,
                published_at_ms: None,
                quality: "unqualified".into(),
                reason_codes: vec!["SYNTHETIC".into()],
                payload: json!({"synthetic_trade":"1"}),
                durable_cursor: None,
            })
            .unwrap();
        let receiver = Arc::new(std::sync::Mutex::new(Some(receiver)));
        let router = axum::Router::new().route(
            "/",
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let app = app.clone();
                let receiver = receiver.lock().unwrap().take().unwrap();
                async move {
                    ws.on_upgrade(move |socket| async move {
                        stream(
                            socket,
                            app,
                            "okx:spot:BTC-USDC".into(),
                            json!({"type":"snapshot","last_update_sequence":1}),
                            receiver,
                        )
                        .await
                        .unwrap();
                    })
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
                .unwrap();
        });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/"))
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        let delivered = timeout(Duration::from_secs(1), async {
            loop {
                let raw = socket.next().await.unwrap().unwrap().into_text().unwrap();
                let value: Value = serde_json::from_str(&raw).unwrap();
                if value.get("payload").is_some() {
                    break value;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(delivered["sequence"], 1);
        assert_eq!(delivered["payload"]["synthetic_trade"], "1");
        socket.close(None).await.unwrap();
        stop.cancel();
        server.await.unwrap();
    }
}
