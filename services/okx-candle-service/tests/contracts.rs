use okx_candle_service::{
    model::{Candle, history_valid, now, parse_row, valid_pair},
    state::State,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::str::FromStr;

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
fn recent() -> u64 {
    (now() as u64 / 60 - 1) * 60
}

#[test]
fn native_captured_parser_parity() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../test/modular_spot/fixtures/rust_candle_parity.json"
    );
    let fixture: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    for case in cases {
        let at = case["row"][0].as_str().unwrap().parse::<f64>().unwrap() / 1000.0 + 120.0;
        let result = parse_row(&case["row"], at);
        match case["outcome"].as_str().unwrap() {
            "ignored" => assert_eq!(result.unwrap(), None),
            "rejected" => assert!(result.is_err(), "{}", case["name"]),
            "accepted" => {
                let got = serde_json::to_value(result.unwrap().unwrap()).unwrap();
                let expected = &case["expected"];
                for field in ["open", "high", "low", "close", "volume", "quote_volume"] {
                    let a = Decimal::from_str(got[field].as_str().unwrap()).unwrap();
                    let b = Decimal::from_scientific(expected[field].as_str().unwrap())
                        .or_else(|_| Decimal::from_str(expected[field].as_str().unwrap()))
                        .unwrap();
                    assert_eq!(a, b, "{} {field}", case["name"]);
                }
                assert_eq!(got["open_time"], expected["open_time"]);
                assert_eq!(got["confirmed"], json!(true));
            }
            _ => panic!("unknown fixture outcome"),
        }
    }
}
#[test]
fn rejects_malformed_precision_geometry_timestamp_and_flags() {
    let base = json!(["1789840020000", "10", "11", "9", "10", "1", "1", "10", "1"]);
    for (index, bad) in [
        (0, "1789840020001"),
        (0, "1.78984002e12"),
        (1, "NaN"),
        (1, "1e1"),
        (1, "0"),
        (1, "0.12345678901234567890123456789012345"),
        (2, "9"),
        (5, "-1"),
        (8, "true"),
    ] {
        let mut row = base.clone();
        row[index] = json!(bad);
        assert!(parse_row(&row, 1789840140.0).is_err(), "{index} {bad}");
    }
    assert!(parse_row(&base, 1789830000.0).is_err());
    assert!(!valid_pair("BTC-USDT"));
    assert!(!valid_pair("BTC-USDC-SWAP"));
    assert!(valid_pair("BTC-USDC"));
}
#[test]
fn history_requires_exact_length_continuity_and_freshness() {
    let at = recent();
    let rows = vec![candle(at - 60), candle(at)];
    assert!(history_valid(&rows, 2, now()));
    assert!(!history_valid(&rows, 3, now()));
    assert!(!history_valid(&[candle(at - 120), candle(at)], 2, now()));
    assert!(!history_valid(&rows, 2, now() + 180.0));
}
#[tokio::test]
async fn sequence_duplicate_correction_gap_and_recovery() {
    let shared = State::new(&["BTC-USDC".into()], 2);
    let mut s = shared.lock().await;
    let at = recent();
    s.acknowledge("BTC-USDC");
    s.apply_seed("BTC-USDC", 0, vec![candle(at - 60), candle(at)], now());
    assert!(s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].sequence, 1);
    s.ingest("BTC-USDC", candle(at), now());
    assert_eq!(s.pairs["BTC-USDC"].sequence, 1);
    let mut correction = candle(at);
    correction.close = "10.5".into();
    s.ingest("BTC-USDC", correction.clone(), now());
    assert_eq!(s.pairs["BTC-USDC"].sequence, 2);
    s.invalidate("BTC-USDC", "test_recovery");
    s.apply_seed("BTC-USDC", 0, vec![candle(at - 60), candle(at)], now());
    assert_eq!(
        s.pairs["BTC-USDC"].rows.back().map(|r| r.as_ref()),
        Some(&correction)
    );
    s.ingest("BTC-USDC", candle(at + 120), now());
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].reason, "candle_gap");
    assert!(s.pairs["BTC-USDC"].rows.len() <= 2);
}
#[tokio::test]
async fn disconnect_blocks_inflight_seed_until_new_generation() {
    let shared = State::new(&["BTC-USDC".into()], 2);
    let mut s = shared.lock().await;
    let at = recent();
    s.acknowledge("BTC-USDC");
    s.disconnected(&["BTC-USDC".into()], "lost");
    s.acknowledge("BTC-USDC");
    s.apply_seed("BTC-USDC", 0, vec![candle(at - 60), candle(at)], now());
    assert!(!s.pairs["BTC-USDC"].ready);
    s.apply_seed("BTC-USDC", 1, vec![candle(at - 60), candle(at)], now());
    assert!(s.pairs["BTC-USDC"].ready);
}
#[tokio::test]
async fn incomplete_seed_fails_closed() {
    let shared = State::new(&["BTC-USDC".into()], 2);
    let mut s = shared.lock().await;
    s.acknowledge("BTC-USDC");
    s.apply_seed("BTC-USDC", 0, vec![candle(recent())], now());
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.counters["seed_failures"], 1);
}

#[tokio::test]
async fn malformed_pair_data_does_not_invalidate_other_pairs() {
    let shared = State::new(&["BTC-USDC".into(), "ETH-USDC".into()], 2);
    let mut s = shared.lock().await;
    let at = recent();
    for pair in ["BTC-USDC", "ETH-USDC"] {
        s.acknowledge(pair);
        s.apply_seed(pair, 0, vec![candle(at - 60), candle(at)], now());
    }
    s.reject_pair_data("BTC-USDC");
    assert!(!s.pairs["BTC-USDC"].ready);
    assert!(s.pairs["ETH-USDC"].ready);
    assert!(s.pairs["BTC-USDC"].online);
    s.apply_seed("BTC-USDC", 0, vec![candle(at - 60), candle(at)], now());
    assert!(!s.pairs["BTC-USDC"].ready);
    s.apply_seed("BTC-USDC", 1, vec![candle(at - 60), candle(at)], now());
    assert!(s.pairs["BTC-USDC"].ready);
}

#[tokio::test]
async fn bounded_broadcast_reports_lag_instead_of_silent_loss() {
    let shared = State::new(&["BTC-USDC".into()], 2);
    let mut s = shared.lock().await;
    let mut receiver = s.events.subscribe();
    for i in 0..okx_candle_service::state::BACKLOG + 1 {
        s.invalidate("BTC-USDC", &format!("failure{i}"));
    }
    assert!(matches!(
        receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
    ));
}

#[test]
fn confirmed_close_in_local_future_is_rejected() {
    let row = json!(["1789840020000", "10", "11", "9", "10", "1", "1", "10", "1"]);
    assert!(parse_row(&row, 1789840079.99).is_err());
    assert!(parse_row(&row, 1789840080.0).unwrap().is_some());
}
