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
                rows: source_rows.into(),
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
