use super::*;
use crate::model::parse_row;
use crate::state::State;
use tokio::io::DuplexStream;
use tokio_tungstenite::tungstenite::protocol::Role;

fn timing() -> SessionTiming {
    SessionTiming {
        acknowledgement: Duration::from_millis(100),
        ping: Duration::from_millis(100),
        pong: Duration::from_millis(75),
        check: Duration::from_millis(5),
    }
}
async fn sockets() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
    let (client, server) = tokio::io::duplex(65536);
    (
        WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        WebSocketStream::from_raw_socket(server, Role::Server, None).await,
    )
}
async fn acknowledge(peer: &mut WebSocketStream<DuplexStream>) {
    let subscribe = peer.next().await.unwrap().unwrap();
    let value: Value = serde_json::from_str(subscribe.to_text().unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"op":"subscribe","args":[{"channel":"candle1m","instId":"BTC-USDC"}]})
    );
    peer.send(Message::Text(
        json!({"event":"subscribe","arg":{"channel":"candle1m","instId":"BTC-USDC"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn busy_data_without_text_pong_still_expires() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        acknowledge(&mut peer).await;
        let ts = (now() as u64 / 60) * 60_000;
        let frame=Message::Text(json!({"arg":{"channel":"candle1m","instId":"BTC-USDC"},"data":[[ts.to_string(),"10","11","9","10","1","1","10","0"]]}).to_string().into());
        let mut busy = interval(Duration::from_millis(2));
        loop {
            tokio::select! {
                _=busy.tick()=> { if peer.send(frame.clone()).await.is_err() { break; } },
                message=peer.next()=> { if message.is_none() || message.is_some_and(|r|r.is_err()) { break; } /* Deliberately ignore text ping. */ }
            }
        }
    });
    let result = timeout(
        Duration::from_secs(2),
        session(state.clone(), &pairs, client, timing()),
    )
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("text pong timeout")
    );
    timeout(Duration::from_secs(1), fake)
        .await
        .unwrap()
        .unwrap();
    let s = state.lock().await;
    assert!(s.pairs["BTC-USDC"].online);
    assert!(s.counters["upstream_frames"] > 10);
    assert_eq!(s.counters["text_pings"], 1);
    assert_eq!(s.counters.get("text_pongs"), None);
}

#[tokio::test]
async fn text_pongs_keep_acknowledged_session_alive_until_close() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        acknowledge(&mut peer).await;
        let mut pings = 0;
        while let Some(message) = peer.next().await {
            if message.unwrap() == Message::Text("ping".into()) {
                pings += 1;
                peer.send(Message::Text("pong".into())).await.unwrap();
                if pings == 3 {
                    peer.send(Message::Close(None)).await.unwrap();
                    break;
                }
            }
        }
        pings
    });
    let result = timeout(
        Duration::from_secs(2),
        session(state.clone(), &pairs, client, timing()),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("upstream close"));
    assert_eq!(fake.await.unwrap(), 3);
    let s = state.lock().await;
    assert_eq!(s.counters["text_pings"], 3);
    assert_eq!(s.counters["text_pongs"], 3);
    assert!(s.pairs["BTC-USDC"].online);
}

#[tokio::test]
async fn missing_acknowledgement_is_bounded() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        while let Some(message) = peer.next().await {
            if message.is_err() {
                break;
            }
        }
    });
    let result = timeout(
        Duration::from_secs(2),
        session(state.clone(), &pairs, client, timing()),
    )
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("subscription acknowledgement timeout")
    );
    timeout(Duration::from_secs(1), fake)
        .await
        .unwrap()
        .unwrap();
    assert!(!state.lock().await.pairs["BTC-USDC"].online);
}

#[tokio::test]
async fn unexpected_eof_errors_and_new_connection_requires_reseed() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        acknowledge(&mut peer).await; /* Drop transport without Close. */
    });
    let result = timeout(
        Duration::from_secs(2),
        session(state.clone(), &pairs, client, timing()),
    )
    .await
    .unwrap();
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("without closing handshake") || error.contains("EOF"),
        "{error}"
    );
    fake.await.unwrap();
    state
        .lock()
        .await
        .disconnected(&pairs, "upstream_disconnected");
    assert!(!state.lock().await.pairs["BTC-USDC"].online);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        acknowledge(&mut peer).await;
        peer.send(Message::Close(None)).await.unwrap();
    });
    let result = timeout(
        Duration::from_secs(2),
        session(state.clone(), &pairs, client, timing()),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().to_string().contains("upstream close"));
    fake.await.unwrap();
    let s = state.lock().await;
    assert!(s.pairs["BTC-USDC"].online);
    assert!(!s.pairs["BTC-USDC"].ready);
    assert_eq!(s.pairs["BTC-USDC"].generation, 1);
}

#[tokio::test]
async fn requested_data_before_ack_is_buffered_without_readiness() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let task_state = state.clone();
    let task_pairs = pairs.clone();
    let collector = tokio::spawn(async move {
        session(task_state, &task_pairs, client, SessionTiming::production()).await
    });
    let subscribe = peer.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(subscribe.to_text().unwrap()).unwrap()["op"],
        "subscribe"
    );
    let at = (now() as u64 / 60 - 1) * 60;
    let raw = json!([
        (at * 1000).to_string(),
        "10",
        "11",
        "9",
        "10",
        "1",
        "1",
        "10",
        "1"
    ]);
    peer.send(Message::Text(
        json!({"arg":{"channel":"candle1m","instId":"BTC-USDC"},"data":[raw.clone()]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if state.lock().await.pairs["BTC-USDC"].rows.len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let s = state.lock().await;
        let p = &s.pairs["BTC-USDC"];
        assert!(!p.online);
        assert!(!p.ready);
        assert_eq!(p.sequence, 0);
        assert_eq!(p.rows.back().unwrap().open_time, at);
        assert_eq!(s.counters["buffered_before_ack"], 1);
    }
    peer.send(Message::Text(
        json!({"event":"subscribe","arg":{"channel":"candle1m","instId":"BTC-USDC"}})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if state.lock().await.pairs["BTC-USDC"].online {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let mut s = state.lock().await;
        assert!(!s.pairs["BTC-USDC"].ready);
        let latest = parse_row(&raw, now()).unwrap().unwrap();
        let mut previous = latest.clone();
        previous.open_time -= 60;
        s.apply_seed("BTC-USDC", 0, vec![previous, latest], now());
        assert!(s.pairs["BTC-USDC"].ready);
        assert_eq!(s.pairs["BTC-USDC"].sequence, 1);
    }
    peer.send(Message::Close(None)).await.unwrap();
    let error = timeout(Duration::from_secs(1), collector)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("upstream close"));
}

#[tokio::test]
async fn unrequested_data_before_ack_is_rejected() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let fake = tokio::spawn(async move {
        peer.next().await.unwrap().unwrap();
        peer.send(Message::Text(
            json!({"arg":{"channel":"candle1m","instId":"ETH-USDC"},"data":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        while let Some(message) = peer.next().await {
            if message.is_err() {
                break;
            }
        }
    });
    let error = timeout(
        Duration::from_secs(1),
        session(state, &pairs, client, timing()),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("unrequested pair"));
    fake.await.unwrap();
}

#[tokio::test]
async fn every_history_page_obeys_shared_pacing_and_offline_fetch_cannot_seed() {
    let at = (now() as u64 / 60 - 1) * 60;
    let make_rows = |n: usize, end: u64| {
        (0..n)
            .map(|i| Candle {
                open_time: end - (n - 1 - i) as u64 * 60,
                open: "10".into(),
                high: "11".into(),
                low: "9".into(),
                close: "10".into(),
                volume: "1".into(),
                quote_volume: "10".into(),
                confirmed: true,
            })
            .collect::<Vec<_>>()
    };
    let pace = Pace::new(Duration::from_millis(10));
    let times = Arc::new(Mutex::new(Vec::new()));
    let fetch = |request: PageRequest| {
        let times = times.clone();
        let end = request.after_ms.map(|ms| ms / 1000 - 60).unwrap_or(at);
        let page = make_rows(request.limit, end);
        async move {
            times.lock().await.push(Instant::now());
            Ok((page, now()))
        }
    };
    let (rows, _) = fetch_history(750, Vec::new(), &pace, fetch).await.unwrap();
    assert_eq!(rows.len(), 750);
    let times = times.lock().await;
    assert_eq!(times.len(), 3);
    assert!(
        times
            .windows(2)
            .all(|w| w[1].duration_since(w[0]) >= Duration::from_millis(10))
    );
    let state = State::new(&["BTC-USDC".into()], 750);
    state.lock().await.apply_seed("BTC-USDC", 0, rows, now());
    assert!(!state.lock().await.pairs["BTC-USDC"].ready);
    let error = fetch_history(750, Vec::new(), &pace, |_| async {
        Err(anyhow::anyhow!("offline"))
    })
    .await
    .unwrap_err();
    assert_eq!(error.to_string(), "offline");
}

#[tokio::test]
async fn actual_session_forming_head_revalidates_cached_history_without_historical_delta() {
    let pairs = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 500);
    let at = (now() as u64 / 60 - 1) * 60;
    let rows = (0..500)
        .map(|i| Candle {
            open_time: at - (499 - i) * 60,
            open: "10".into(),
            high: "11".into(),
            low: "9".into(),
            close: "10".into(),
            volume: "1".into(),
            quote_volume: "10".into(),
            confirmed: true,
        })
        .collect();
    state.lock().await.set_seed_base("BTC-USDC", rows);
    let mut events = state.lock().await.events.subscribe();
    let (client, mut peer) = sockets().await;
    let task_state = state.clone();
    let collector = tokio::spawn(async move {
        session(task_state, &pairs, client, SessionTiming::production()).await
    });
    acknowledge(&mut peer).await;
    peer.send(Message::Text(json!({"arg":{"channel":"candle1m","instId":"BTC-USDC"},"data":[[((at+60)*1000).to_string(),"10","11","9","10","1","1","10","0"]]}).to_string().into())).await.unwrap();
    let frame = timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap()
        .into_frame(500);
    assert_eq!(frame["type"], "snapshot");
    assert_eq!(frame["rows"].as_array().unwrap().len(), 500);
    assert_eq!(frame["rows"][499]["open_time"], at);
    assert!(frame["received_at"].as_f64().unwrap() > 0.0);
    {
        let s = state.lock().await;
        assert!(s.pairs["BTC-USDC"].ready);
        assert_eq!(s.counters["warm_restores"], 1);
        assert!(!s.counters.contains_key("rest_requests"));
        assert!(!s.counters.contains_key("candle_events"));
    }
    peer.send(Message::Close(None)).await.unwrap();
    assert!(collector.await.unwrap().is_err());
}

fn confirmed_frame(pair: &str, open_time: u64) -> Message {
    Message::Text(
        json!({"arg":{"channel":"candle1m","instId":pair},"data":[[(open_time * 1000).to_string(),"10","11","9","10","1","1","10","1"]]})
            .to_string()
            .into(),
    )
}

#[tokio::test]
async fn two_lanes_deduplicate_and_one_lane_reset_never_opens_a_gap() {
    let pairs: Vec<String> = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let closed = (now() as u64 / 60 - 1) * 60;
    let mut peers = Vec::new();
    let mut sessions = Vec::new();
    for lane in 0..2u8 {
        let (client, mut peer) = sockets().await;
        let (state, pairs) = (state.clone(), pairs.clone());
        sessions.push(tokio::spawn(async move {
            session_lane(state, &pairs, client, SessionTiming::production(), lane).await
        }));
        acknowledge(&mut peer).await;
        peer.send(confirmed_frame("BTC-USDC", closed))
            .await
            .unwrap();
        peers.push(peer);
    }
    timeout(Duration::from_secs(2), async {
        loop {
            let s = state.lock().await;
            if s.counters.get("confirmed_rows") == Some(&2) {
                break;
            }
            drop(s);
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    {
        let mut s = state.lock().await;
        let p = &s.pairs["BTC-USDC"];
        assert_eq!(p.lanes, 0b11, "both lanes acknowledged");
        assert_eq!(p.rows.len(), 1, "identical rows from two lanes merge");
        assert_eq!(s.counters["duplicates_ignored"], 1);
        // Make the pair ready from a complete contiguous seed, as the repair loop would.
        let generation = s.pairs["BTC-USDC"].generation;
        let rows = vec![
            crate::model::Candle {
                open_time: closed - 60,
                open: "10".into(),
                high: "11".into(),
                low: "9".into(),
                close: "10".into(),
                volume: "1".into(),
                quote_volume: "10".into(),
                confirmed: true,
            },
            s.pairs["BTC-USDC"].rows[0].as_ref().clone(),
        ];
        assert!(s.apply_seed("BTC-USDC", generation, rows, now()));
    }
    // Lane 0 resets. The pair must remain online and ready: no invalidation, no new epoch work.
    drop(peers.remove(0));
    let lane0 = sessions.remove(0).await.unwrap();
    assert!(lane0.is_err());
    state
        .lock()
        .await
        .lane_disconnected(&pairs, 0, "upstream_disconnected");
    {
        let s = state.lock().await;
        let p = &s.pairs["BTC-USDC"];
        assert!(p.online && p.ready, "surviving lane keeps the pair live");
        assert_eq!(p.lanes, 0b10);
        assert_eq!(p.reconnects, 1);
        assert_eq!(s.counters.get("invalidations"), None);
    }
    // The surviving lane keeps publishing: a latest-bar correction is a normal delta event.
    peers[0]
        .send(Message::Text(
            json!({"arg":{"channel":"candle1m","instId":"BTC-USDC"},"data":[[(closed * 1000).to_string(),"10","11","9","10.5","1","1","10","1"]]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if state.lock().await.counters.get("candle_events") == Some(&1) {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // Only when the last lane is lost does the pair take the full disconnect path.
    drop(peers.remove(0));
    assert!(sessions.remove(0).await.unwrap().is_err());
    state
        .lock()
        .await
        .lane_disconnected(&pairs, 1, "upstream_disconnected");
    let s = state.lock().await;
    assert!(!s.pairs["BTC-USDC"].online && !s.pairs["BTC-USDC"].ready);
    assert_eq!(s.counters["invalidations"], 1);
}

#[tokio::test]
async fn skewed_confirmed_and_future_forming_rows_are_classified_not_fatal() {
    let pairs: Vec<String> = vec!["BTC-USDC".into()];
    let state = State::new(&pairs, 2);
    let (client, mut peer) = sockets().await;
    let (task_state, task_pairs) = (state.clone(), pairs.clone());
    let collector = tokio::spawn(async move {
        session(task_state, &task_pairs, client, SessionTiming::production()).await
    });
    acknowledge(&mut peer).await;
    let next_minute = (now() as u64 / 60 + 1) * 60;
    // A confirmed bar closing a minute ahead of the local clock and a forming bar two minutes
    // ahead: before this change either one invalidated the pair and wiped its baseline.
    let frame = json!({"arg":{"channel":"candle1m","instId":"BTC-USDC"},"data":[
        [((next_minute - 60) * 1000).to_string(),"10","11","9","10","1","1","10","1"],
        [((next_minute + 60) * 1000).to_string(),"10","11","9","10","1","1","10","0"],
        [((now() as u64 / 60) * 60 * 1000).to_string(),"10","9","11","10","1","1","10","0"]
    ]});
    peer.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            if state.lock().await.counters.get("upstream_frames") == Some(&1) {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let s = state.lock().await;
    let p = &s.pairs["BTC-USDC"];
    assert_eq!(s.counters.get("parser_failures"), None);
    assert_eq!(s.counters.get("invalidations"), None);
    assert_eq!(p.generation, 0, "no generation bump, baseline untouched");
    assert_eq!(p.deferred.len(), 1);
    assert_eq!(p.rejected["clock_skew_deferred"], 1);
    assert_eq!(p.rejected["forming_future"], 1);
    assert_eq!(p.rejected["forming_malformed"], 1);
    assert!(
        p.live_head.is_none(),
        "future forming bars never become a live head"
    );
    drop(s);
    collector.abort();
}
