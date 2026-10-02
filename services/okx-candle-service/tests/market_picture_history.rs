use okx_candle_service::{
    market_picture::{MINUTE_ROWS, Projection, SharedProjection},
    model::{Candle, now},
    server,
    state::State,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

fn confirmed_rows() -> Vec<Arc<Candle>> {
    let latest = (now() as u64 / 60 - 1) * 60;
    let first = latest - (MINUTE_ROWS as u64 - 1) * 60;
    (0..MINUTE_ROWS)
        .map(|index| {
            Arc::new(Candle {
                open_time: first + index as u64 * 60,
                open: "1000000000000000000000000".into(),
                high: "1000000000000000000000001".into(),
                low: "1000000000000000000000000".into(),
                close: "1000000000000000000000001".into(),
                volume: "0.125".into(),
                quote_volume: "125000000000000000000000.125".into(),
                confirmed: true,
            })
        })
        .collect()
}

#[tokio::test]
async fn market_picture_read_route_is_auth_bound_compact_and_keeps_v1_separate() {
    let now_seconds = now();
    let now_ms = (now_seconds * 1000.0).round() as u64;
    let pair = "BTC-USDC".to_owned();
    let source_epoch = "a1234567-1234-4123-8123-123456789abc";
    let source_rows = confirmed_rows();
    let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
    projection
        .observe(
            okx_candle_service::state::Event::Snapshot {
                metadata: json!({
                    "v": 1,
                    "type": "snapshot",
                    "epoch": source_epoch,
                    "published_at": now_seconds,
                    "pair": pair,
                    "sequence": 1,
                    "received_at": now_seconds,
                }),
                rows: source_rows.clone().into(),
            },
            now_ms,
        )
        .unwrap();
    projection
        .response(
            &pair,
            okx_candle_service::market_picture::Interval::Minute,
            now_ms,
        )
        .unwrap();

    let projection = Arc::new(Mutex::new(projection)) as SharedProjection;
    let projection_writer = projection.clone();
    let source_state = State::new(std::slice::from_ref(&pair), 90);
    let stop = CancellationToken::new();
    let token = "0123456789abcdef0123456789abcdef";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router(source_state, token.into(), stop.clone(), Some(projection));
    let server_stop = stop.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_stop.cancelled_owned())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let route = format!("http://{addr}/v2/market-picture/history?pair=BTC-USDC&interval=1m");
    assert_eq!(client.get(&route).send().await.unwrap().status(), 401);
    let response = client.get(&route).bearer_auth(token).send().await.unwrap();
    assert_eq!(response.status(), 200);
    let bytes = response.bytes().await.unwrap();
    assert!(bytes.len() <= 8_000_000);
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["schema_version"], "market-picture.source-history.v1");
    assert_eq!(body["encoding"], "ohlcv-tuples.v1");
    assert_eq!(body["source"]["venue"], "okx");
    assert_eq!(body["source"]["market_type"], "spot");
    assert_eq!(body["source"]["quote_asset"], "USDC");
    assert_eq!(body["ready"], true);
    assert_eq!(body["coverage"]["expected_count"], MINUTE_ROWS);
    assert_eq!(body["bars"].as_array().unwrap().len(), MINUTE_ROWS);
    assert_eq!(body["bars"][0].as_array().unwrap().len(), 9);
    assert_eq!(body["bars"][0][0], "1000000000000000000000000");
    assert_eq!(body["bars"][0][7], 0);
    assert_eq!(body["bars"][0][8], 1);

    let updates_route = format!("http://{addr}/v2/market-picture/updates");
    let unauthorized_updates = client
        .get(&updates_route)
        .query(&[("pair", "BTC-USDC"), ("interval", "1m")])
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized_updates.status(), 401);
    let full = client
        .get(&updates_route)
        .query(&[("pair", "BTC-USDC"), ("interval", "1m")])
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(full.status(), 200);
    let full: Value = full.json().await.unwrap();
    assert_eq!(full["schema_version"], "market-picture.source-updates.v1");
    assert_eq!(full["mode"], "full");
    assert_eq!(full["from_sequence"], Value::Null);
    assert_eq!(full["bars"].as_array().unwrap().len(), MINUTE_ROWS);
    let projection_epoch = full["epoch"].as_str().unwrap().to_owned();
    let mut cursor = full["sequence"].as_u64().unwrap();
    let stable_available_at = full["available_at_ms"].as_u64().unwrap();
    let cursor_text = cursor.to_string();
    let empty = client
        .get(&updates_route)
        .query(&[
            ("pair", "BTC-USDC"),
            ("interval", "1m"),
            ("epoch", projection_epoch.as_str()),
            ("sequence", cursor_text.as_str()),
        ])
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 200);
    let empty: Value = empty.json().await.unwrap();
    assert_eq!(empty["mode"], "delta");
    assert_eq!(empty["from_sequence"], cursor);
    assert!(empty["upserts"].as_array().unwrap().is_empty());
    assert!(empty["deleted_open_time_ms"].as_array().unwrap().is_empty());
    assert!(empty["source_epochs"].as_array().unwrap().is_empty());

    let mut corrected = (*source_rows.last().unwrap().as_ref()).clone();
    corrected.close = "1000000000000000000000000".into();
    let latest_open = corrected.open_time;
    projection_writer
        .lock()
        .await
        .observe(
            okx_candle_service::state::Event::Other(json!({
                "v": 1,
                "type": "candle",
                "epoch": source_epoch,
                "pair": pair,
                "sequence": 2,
                "published_at": now_seconds,
                "received_at": now_seconds,
                "row": corrected,
            })),
            now_ms,
        )
        .unwrap();
    let cursor_text = cursor.to_string();
    let correction = client
        .get(&updates_route)
        .query(&[
            ("pair", "BTC-USDC"),
            ("interval", "1m"),
            ("epoch", projection_epoch.as_str()),
            ("sequence", cursor_text.as_str()),
        ])
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(correction.status(), 200);
    let correction: Value = correction.json().await.unwrap();
    assert_eq!(correction["mode"], "delta");
    assert_eq!(correction["from_sequence"], cursor);
    assert_eq!(correction["upserts"].as_array().unwrap().len(), 1);
    assert_eq!(correction["upserts"][0]["open_time_ms"], latest_open * 1000);
    assert_eq!(
        correction["upserts"][0]["close"],
        "1000000000000000000000000"
    );
    cursor = correction["sequence"].as_u64().unwrap();
    assert!(cursor > full["sequence"].as_u64().unwrap());
    assert_eq!(correction["available_at_ms"], stable_available_at);

    let future_cursor_text = cursor.saturating_add(1).to_string();
    let bad_cursor = client
        .get(&updates_route)
        .query(&[
            ("pair", "BTC-USDC"),
            ("interval", "1m"),
            ("epoch", projection_epoch.as_str()),
            ("sequence", future_cursor_text.as_str()),
        ])
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(bad_cursor.status(), 200);
    assert_eq!(bad_cursor.json::<Value>().await.unwrap()["mode"], "full");

    let partial_cursor = client
        .get(&updates_route)
        .query(&[("pair", "BTC-USDC"), ("interval", "1m"), ("epoch", "only")])
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(partial_cursor.status(), 400);

    let unknown = client
        .get(format!(
            "http://{addr}/v2/market-picture/history?pair=ETH-USDC&interval=1m"
        ))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 404);
    stop.cancel();
    server.await.unwrap();
}

#[tokio::test]
async fn deferred_close_event_preserves_receipt_and_projects_availability_at_release() {
    use okx_candle_service::market_picture::{Interval, MINUTE_ROWS};
    use okx_candle_service::state::Event;

    let pair = "BTC-USDC".to_owned();
    let now_seconds = now();
    let now_ms = (now_seconds * 1000.0).round() as u64;
    let close = (now_seconds as u64 / 60 + 1) * 60;
    let rows: Vec<Candle> = confirmed_rows()
        .into_iter()
        .map(|row| row.as_ref().clone())
        .collect();
    assert_eq!(rows.len(), MINUTE_ROWS);

    let state = State::new(std::slice::from_ref(&pair), MINUTE_ROWS);
    let mut projection = Projection::new(std::slice::from_ref(&pair), now_ms - 1_000);
    let mut events = state.lock().await.events.subscribe();
    {
        let mut source = state.lock().await;
        source.acknowledge(&pair);
        assert!(
            source.apply_seed(&pair, 0, rows, now_seconds),
            "seed not ready: {}",
            source.pairs[&pair].reason
        );
    }
    let snapshot = events.try_recv().unwrap();
    projection
        .observe(snapshot, (now() * 1000.0).round() as u64)
        .unwrap();

    let original_received_at = close as f64 - 0.1;
    let next = Candle {
        open_time: close - 60,
        open: "1000000000000000000000000".into(),
        high: "1000000000000000000000001".into(),
        low: "1000000000000000000000000".into(),
        close: "1000000000000000000000001".into(),
        volume: "0.125".into(),
        quote_volume: "125000000000000000000000.125".into(),
        confirmed: true,
    };
    {
        let mut source = state.lock().await;
        source.defer(&pair, next, original_received_at);
        source.release_deferred(close as f64 - 0.001);
        assert_eq!(source.pairs[&pair].deferred.len(), 1);
        assert_eq!(source.counters.get("candle_events"), None);
        source.release_deferred(close as f64);
        assert_eq!(source.counters["deferred_released"], 1);
    }
    let event = events.try_recv().unwrap();
    assert!(matches!(event, Event::Other(_)));
    assert_eq!(event.metadata()["received_at"], json!(original_received_at));
    assert_eq!(event.metadata()["published_at"], json!(close as f64));
    projection.observe(event, close * 1000).unwrap();

    let response = projection
        .response(&pair, Interval::Minute, close * 1000)
        .unwrap();
    assert!(response.ready);
    assert_eq!(response.available_at_ms, close * 1000);
    assert_eq!(response.coverage.valid_count, MINUTE_ROWS);
}

#[test]
fn deferred_event_uses_actual_publication_for_freshness_without_rewriting_receipt() {
    use okx_candle_service::market_picture::{Interval, MINUTE_ROWS};
    use okx_candle_service::state::Event;

    let pair = "BTC-USDC".to_owned();
    let wall_now_ms = (now() * 1000.0).round() as u64;
    let close_ms = wall_now_ms / 60_000 * 60_000 - 120_000;
    let early_received_ms = close_ms - 120_000;
    let snapshot_at_ms = close_ms - 60_000;
    let open_ms = close_ms - 60_000;
    let first_open_ms = open_ms - (MINUTE_ROWS as u64 - 1) * 60_000;
    let rows: Vec<Candle> = (0..MINUTE_ROWS)
        .map(|index| Candle {
            open_time: (first_open_ms / 1000) + index as u64 * 60,
            open: "100".into(),
            high: "101".into(),
            low: "99".into(),
            close: "100".into(),
            volume: "1".into(),
            quote_volume: "100".into(),
            confirmed: true,
        })
        .collect();
    let source_epoch = "deferred-test";
    let mut projection = Projection::new(std::slice::from_ref(&pair), close_ms - 180_000);
    projection
        .observe(
            Event::Snapshot {
                metadata: json!({
                    "v": 1,
                    "type": "snapshot",
                    "epoch": source_epoch,
                    "pair": pair,
                    "sequence": 1,
                    "published_at": snapshot_at_ms as f64 / 1000.0,
                    "received_at": snapshot_at_ms as f64 / 1000.0,
                }),
                rows: rows.into_iter().map(Arc::new).collect(),
            },
            snapshot_at_ms,
        )
        .unwrap();

    let deferred_close = Candle {
        open_time: open_ms / 1000,
        open: "100".into(),
        high: "101".into(),
        low: "99".into(),
        close: "100.5".into(),
        volume: "1".into(),
        quote_volume: "100.5".into(),
        confirmed: true,
    };
    let published_at = close_ms as f64 / 1000.0;
    projection
        .observe(
            Event::Other(json!({
                "v": 1,
                "type": "candle",
                "epoch": source_epoch,
                "pair": pair,
                "sequence": 2,
                "received_at": early_received_ms as f64 / 1000.0,
                "published_at": published_at,
                "row": deferred_close,
            })),
            close_ms,
        )
        .unwrap();

    let ready = projection
        .response("BTC-USDC", Interval::Minute, close_ms)
        .unwrap();
    assert!(ready.ready, "actual close publication restores freshness");
    assert_eq!(ready.bars.len(), MINUTE_ROWS);
    assert_eq!(ready.bars.last().unwrap().6, close_ms);
    assert_eq!(ready.available_at_ms, close_ms);

    let mut next_delta = deferred_close;
    next_delta.close = "100.25".into();
    projection
        .observe(
            Event::Other(json!({
                "v": 1,
                "type": "candle",
                "epoch": source_epoch,
                "pair": "BTC-USDC",
                "sequence": 3,
                "received_at": published_at,
                "published_at": published_at,
                "row": next_delta,
            })),
            close_ms + 1_000,
        )
        .unwrap();
    let after_delta = projection
        .response("BTC-USDC", Interval::Minute, close_ms + 1_000)
        .unwrap();
    assert!(after_delta.ready, "the next source delta is not stranded");
    assert_eq!(after_delta.sequence, 3);

    let future_publication = close_ms + 2_000;
    let error = projection
        .observe(
            Event::Other(json!({
                "v": 1,
                "type": "candle",
                "epoch": source_epoch,
                "pair": "BTC-USDC",
                "sequence": 4,
                "received_at": published_at,
                "published_at": future_publication as f64 / 1000.0,
                "row": next_delta,
            })),
            close_ms + 1_000,
        )
        .unwrap_err();
    assert!(error.to_string().contains("future dated"));
}

#[tokio::test]
async fn writes_sanitized_actual_producer_wire_fixture_when_requested() {
    use okx_candle_service::state::Event;

    let pair = "BTC-USDC".to_owned();
    let now_seconds = now();
    let now_ms = (now_seconds * 1000.0).round() as u64;
    let current_minute = now_seconds as u64 / 60 * 60;
    let latest_open = current_minute - 60;
    let rows: Vec<Candle> = (0..60)
        .map(|index| Candle {
            open_time: latest_open - (59 - index) * 60,
            open: "100".into(),
            high: "101".into(),
            low: "99".into(),
            close: "100".into(),
            volume: "1".into(),
            quote_volume: "100".into(),
            confirmed: true,
        })
        .collect();
    let state = State::new(std::slice::from_ref(&pair), 60);
    let mut events = state.lock().await.events.subscribe();
    {
        let mut source = state.lock().await;
        source.acknowledge(&pair);
        assert!(
            source.apply_seed(&pair, 0, rows, now_seconds),
            "seed not ready: {}",
            source.pairs[&pair].reason
        );
    }
    let snapshot = events.try_recv().unwrap();
    let snapshot_frame = snapshot.clone().into_frame(60);

    let next = Candle {
        open_time: current_minute - 60,
        open: "100".into(),
        high: "101".into(),
        low: "99".into(),
        close: "100.5".into(),
        volume: "1".into(),
        quote_volume: "100.5".into(),
        confirmed: true,
    };
    let original_received_at = current_minute as f64 - 0.1;
    {
        let mut source = state.lock().await;
        source.defer(&pair, next, original_received_at);
        source.release_deferred(now());
    }
    let deferred = events.try_recv().unwrap();
    assert!(matches!(deferred, Event::Other(_)));
    assert!(deferred.metadata()["published_at"].as_f64().unwrap() >= current_minute as f64);
    assert!(
        deferred.metadata()["published_at"].as_f64().unwrap()
            >= snapshot_frame["published_at"].as_f64().unwrap()
    );
    assert_eq!(
        deferred.metadata()["received_at"],
        json!(original_received_at)
    );

    if std::env::var_os("RSIBOT_WRITE_DEFERRED_FIXTURE").is_some() {
        let path = "docs/evidence/2026-10-02-confirmed-close-availability/producer-events.json";
        std::fs::create_dir_all("docs/evidence/2026-10-02-confirmed-close-availability").unwrap();
        let fixture = json!({
            "fixture": "sanitized actual State producer snapshot and deferred candle event",
            "generated_at_ms": now_ms,
            "snapshot": snapshot_frame,
            "deferred_candle": deferred.metadata(),
        });
        std::fs::write(path, serde_json::to_vec_pretty(&fixture).unwrap()).unwrap();
    }
}
