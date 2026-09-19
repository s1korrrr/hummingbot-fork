use futures_util::{SinkExt, StreamExt};
use okx_candle_service::{
    model::{Candle, now},
    server,
    state::State,
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn authenticated_snapshot_delta_status_heartbeat_and_shutdown() {
    let state = State::new(&["BTC-USDC".into()], 2);
    let stop = CancellationToken::new();
    let token = "0123456789abcdef0123456789abcdef";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server::router(state.clone(), token.into(), stop.clone());
    let stop_server = stop.clone();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(stop_server.cancelled_owned())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    assert_eq!(
        client
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("http://{addr}/health"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert!(
        connect_async(format!("ws://{addr}/v1/stream"))
            .await
            .is_err()
    );
    let mut request = format!("ws://{addr}/v1/stream")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            json!({"v":1,"op":"subscribe","pairs":["BTC-USDC"],"history":2})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let frame: Value =
        serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
    assert_eq!(frame["type"], "status");
    assert_eq!(frame["sequence"], 0);
    let at = (now() as u64 / 60 - 1) * 60;
    let row = |t| Candle {
        open_time: t,
        open: "10".into(),
        high: "11".into(),
        low: "9".into(),
        close: "10".into(),
        volume: "1".into(),
        quote_volume: "10".into(),
        confirmed: true,
    };
    {
        let mut s = state.lock().await;
        s.acknowledge("BTC-USDC");
        s.apply_seed("BTC-USDC", 0, vec![row(at - 60), row(at)], now());
        let mut correction = row(at);
        correction.close = "10.5".into();
        s.ingest("BTC-USDC", correction, now());
        s.invalidate("BTC-USDC", "test_gap");
    }
    let mut sequence = 0;
    let mut types = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            if frame["type"] == "heartbeat" {
                assert_eq!(frame["states"]["BTC-USDC"]["sequence"], sequence);
                if sequence == 3 {
                    break;
                }
            } else {
                sequence += 1;
                assert_eq!(frame["sequence"], sequence);
                types.push(frame["type"].as_str().unwrap().to_string());
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(types, ["snapshot", "candle", "status"]);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(3), handle)
        .await
        .unwrap()
        .unwrap();
}
