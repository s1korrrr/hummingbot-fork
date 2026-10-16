use futures_util::StreamExt;
use okx_candle_service::{
    auth::{Auth, Consumer},
    observation_capture::{CaptureState, ObservationUpdate},
    observation_config::ObservationSettings,
    server,
    state::State,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use tokio_util::sync::CancellationToken;

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

fn settings() -> ObservationSettings {
    let instruments = ["BTC", "ETH"].map(|asset| json!({
        "inst_id":format!("{asset}-USDC"),"instrument_id":format!("okx:spot:{asset}-USDC"),
        "metadata_revision":"synthetic-meta","tick_size":"0.01","lot_size":"0.01","minimum_size":"0.01",
        "effective_at_ms":1,"available_at_ms":1
    }));
    serde_json::from_value(json!({
        "schema_version":"okx.observations.config.v1","enabled":true,"environment":"public","region":"eea",
        "instruments":instruments,
        "channels":["trades-all","bbo-tbt"],"journal_dir":"/unused-not-opened","notional_bars":[],
        "limits":{"max_pairs":2,"max_sockets":2,"max_subscriptions":4,"max_frame_bytes":65536,
            "queue_capacity":4,"dedup_identities":100,"max_replay_records":1000,"metadata_refresh_interval_ms":60000,"segment_bytes":16777216,"minimum_disk_free_bytes":1,
            "raw_retention_days":1}
    })).unwrap()
}

fn update(pair: &str) -> ObservationUpdate {
    ObservationUpdate {
        schema_version: String::new(),
        sequence: 0,
        epoch: "synthetic-epoch".into(),
        record_kind: "trade".into(),
        channel: Some("trades-all".into()),
        instrument_id: Some(format!("okx:spot:{pair}")),
        available_at_ms: Some(100),
        accepted_at_ms: None,
        durable_acknowledged_at_ms: None,
        computed_at_ms: None,
        published_at_ms: None,
        quality: "unqualified".into(),
        reason_codes: vec!["SYNTHETIC".into()],
        payload: json!({"instrument_id":format!("okx:spot:{pair}"),"synthetic":true}),
        durable_cursor: None,
    }
}

async fn serve(
    capture: Option<Arc<CaptureState>>,
    auth: Auth,
) -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
    let stop = CancellationToken::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let app = server::router_with_observations(
        State::new(&["BTC-USDC".into(), "ETH-USDC".into()], 2),
        auth,
        stop.clone(),
        None,
        capture,
    );
    let shutdown = stop.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .unwrap();
    });
    (address, stop, task)
}

fn request(address: &str, query: &str) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut req = format!("ws://{address}/v2/observations/stream?{query}")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    req
}

#[tokio::test]
async fn disabled_extension_requires_auth_and_never_activates_from_reads() {
    let (address, stop, task) = serve(None, Auth::single(TOKEN.into(), 2, 1)).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    for route in ["health", "capabilities"] {
        let url = format!("http://{address}/v2/observations/{route}");
        assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
        let value: Value = client
            .get(&url)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value["enabled"], false);
        assert_eq!(value["capital_authority"], false);
        assert_eq!(
            client
                .get(&url)
                .bearer_auth(TOKEN)
                .header("origin", "https://example.invalid")
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
    }
    assert!(
        connect_async(request(&address, "pair=BTC-USDC"))
            .await
            .is_err()
    );
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn scoped_stream_filters_pairs_bounds_consumers_and_declares_cursor_reset() {
    let capture = CaptureState::new(&settings()).unwrap();
    let consumer = Consumer::new(
        "scoped",
        TOKEN,
        Some(BTreeSet::from(["BTC-USDC".into()])),
        2,
        1,
    );
    let candle_slots = consumer.streams.clone();
    let (address, stop, task) = serve(Some(capture.clone()), Auth::new(vec![consumer])).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let caps: Value = client
        .get(format!("http://{address}/v2/observations/capabilities"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(caps["instrument_ids"], json!(["okx:spot:BTC-USDC"]));
    assert_eq!(caps["passive_queue_qualified"], false);
    for (query, status) in [
        ("pair=ETH-USDC", 403),
        ("pair=BTC-USDC&epoch=e&sequence=1", 410),
    ] {
        let error = connect_async(request(&address, query)).await.unwrap_err();
        match error {
            tokio_tungstenite::tungstenite::Error::Http(response) => {
                assert_eq!(response.status(), status)
            }
            other => panic!("{other}"),
        }
    }
    let (mut socket, _) = connect_async(request(&address, "pair=BTC-USDC"))
        .await
        .unwrap();
    let first = socket.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&first).unwrap()["type"],
        "snapshot"
    );
    assert_eq!(
        candle_slots.available_permits(),
        1,
        "observations cannot consume candle stream permits"
    );
    assert!(
        connect_async(request(&address, "pair=BTC-USDC"))
            .await
            .is_err()
    );
    capture.publish(update("ETH-USDC")).unwrap();
    capture.publish(update("BTC-USDC")).unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&frame).unwrap();
            if value.get("payload").is_some() {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(seen["instrument_id"], "okx:spot:BTC-USDC");
    assert!(seen["durable_cursor"].is_null());
    socket.close(None).await.unwrap();
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn lagged_reader_receives_explicit_reset_without_stalling_capture() {
    let capture = CaptureState::new(&settings()).unwrap();
    let (address, stop, task) =
        serve(Some(capture.clone()), Auth::single(TOKEN.into(), 2, 1)).await;
    let (mut socket, _) = connect_async(request(&address, "pair=BTC-USDC"))
        .await
        .unwrap();
    socket.next().await.unwrap().unwrap();
    // No await in this burst: bounded broadcast must report the lost range.
    for _ in 0..100 {
        capture.publish(update("BTC-USDC")).unwrap();
    }
    let reset = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&frame).unwrap();
            if value["type"] == "reset" {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reset["reason"], "MISSED_UPDATES");
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn connection_loss_reaches_scoped_reader_without_other_pair_payloads() {
    let capture = CaptureState::new(&settings()).unwrap();
    let consumer = Consumer::new(
        "scoped",
        TOKEN,
        Some(BTreeSet::from(["BTC-USDC".into()])),
        2,
        1,
    );
    let (address, stop, task) = serve(Some(capture.clone()), Auth::new(vec![consumer])).await;
    let (mut socket, _) = connect_async(request(&address, "pair=BTC-USDC"))
        .await
        .unwrap();
    socket.next().await.unwrap().unwrap();
    let mut disconnect = update("BTC-USDC");
    disconnect.instrument_id = None;
    disconnect.record_kind = "disconnect".into();
    disconnect.reason_codes = vec!["SOURCE_DISCONNECTED".into()];
    disconnect.payload = json!({"raw_payload":"ETH-USDC-private-rejection","event":"disconnect"});
    capture.publish(disconnect).unwrap();
    let status = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            let frame = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let value: Value = serde_json::from_str(&frame).unwrap();
            if value["type"] == "source_status" {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(status["reason"], "SOURCE_DISCONNECTED");
    assert!(!status.to_string().contains("ETH-USDC"));
    socket.close(None).await.unwrap();
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn bounded_observation_burst_keeps_candle_transport_responsive() {
    // Engineering envelope declared before the burst: 10,000 bounded updates,
    // one unread observation client, and two seconds for the candle handshake.
    // This is a localhost transport smoke, not a production CPU/RSS benchmark.
    let capture = CaptureState::new(&settings()).unwrap();
    let consumer = Consumer::new("load", TOKEN, None, 2, 1);
    let candle_slots = consumer.streams.clone();
    let (address, stop, task) = serve(Some(capture.clone()), Auth::new(vec![consumer])).await;
    let (mut slow, _) = connect_async(request(&address, "pair=BTC-USDC"))
        .await
        .unwrap();
    slow.next().await.unwrap().unwrap();
    let producer = tokio::spawn(async move {
        for batch in 0..100 {
            for _ in 0..100 {
                capture
                    .publish(update(if batch % 2 == 0 {
                        "BTC-USDC"
                    } else {
                        "ETH-USDC"
                    }))
                    .unwrap();
            }
            tokio::task::yield_now().await;
        }
        capture.health_snapshot().unwrap().last_update_sequence
    });
    let mut candle_request = format!("ws://{address}/v1/stream")
        .into_client_request()
        .unwrap();
    candle_request
        .headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let (mut candle, _) =
        tokio::time::timeout(Duration::from_secs(2), connect_async(candle_request))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(candle_slots.available_permits(), 0);
    assert_eq!(producer.await.unwrap(), 10_000);
    candle.close(None).await.unwrap();
    // Observation transport may already have disconnected the lagging client.
    let _ = slow.close(None).await;
    stop.cancel();
    task.await.unwrap();
}

#[tokio::test]
async fn scoped_channel_timer_carries_durable_time_without_price_or_other_pair_data() {
    let capture = CaptureState::new(&settings()).unwrap();
    let (address, stop, task) =
        serve(Some(capture.clone()), Auth::single(TOKEN.into(), 2, 1)).await;
    let (mut socket, _) = connect_async(request(&address, "pair=BTC-USDC"))
        .await
        .unwrap();
    socket.next().await.unwrap().unwrap();
    let mut timer = update("BTC-USDC");
    timer.instrument_id = None;
    timer.record_kind = "timer".into();
    timer.available_at_ms = Some(60_000);
    timer.payload = json!({"event":"timer","other_pair":"ETH-USDC","bid_price":"1",
        "coverage_by_instrument":{"okx:spot:BTC-USDC":{"qualified":true,"coverage_start_ms":1},
            "okx:spot:ETH-USDC":{"qualified":true,"coverage_start_ms":1}}});
    timer.durable_cursor = Some(okx_candle_service::observation_journal::JournalCursor {
        segment: 1,
        sequence: 7,
        hash: "a".repeat(64),
    });
    capture.publish(timer).unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value: Value =
                serde_json::from_str(&socket.next().await.unwrap().unwrap().into_text().unwrap())
                    .unwrap();
            if value["record_kind"] == "timer" {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(seen["available_at_ms"], 60_000);
    assert_eq!(seen["durable_cursor"]["sequence"], 7);
    assert_eq!(seen["instrument_id"], "okx:spot:BTC-USDC");
    assert_eq!(seen["payload"]["qualified_source"], false);
    assert_eq!(
        seen["payload"]["coverage_by_instrument"]
            .as_object()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        seen["payload"]["coverage_by_instrument"]["okx:spot:BTC-USDC"]["qualified"],
        true
    );
    assert!(!seen.to_string().contains("ETH-USDC"));
    assert!(!seen.to_string().contains("bid_price"));
    socket.close(None).await.unwrap();
    stop.cancel();
    task.await.unwrap();
}
