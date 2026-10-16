use crate::observation_journal::JournalCursor;
use crate::observations::{
    DedupDecision, NotionalBarBuilder, ObservationReplay, ObservationTrade, TradeDeduplicator,
    TradeInput, parse_trade_payload,
};
use rust_decimal::Decimal;
use serde_json::json;

fn trade(id: &str, px: &str, sz: &str) -> ObservationTrade {
    ObservationTrade::new(TradeInput {
        instrument_id: "okx:spot:BTC-USDC".into(),
        channel: "trades-all".into(),
        trade_id: id.into(),
        price: px.into(),
        base_quantity: sz.into(),
        side: "buy".into(),
        event_at_ms: 1,
        available_at_ms: 1,
    })
    .unwrap()
}

#[test]
fn notional_bars_keep_whole_crossing_trade_and_do_not_carry_overshoot() {
    let mut bars = NotionalBarBuilder::new(Decimal::new(100, 0)).unwrap();
    assert!(bars.push(trade("1", "10", "3")).unwrap().is_empty());
    let first = bars.push(trade("2", "10", "8")).unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].quote_notional, Decimal::new(110, 0));
    assert_eq!(first[0].overshoot, Decimal::new(10, 0));
    let second = bars.push(trade("3", "10", "25")).unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].quote_notional, Decimal::new(250, 0));
    assert_eq!(second[0].overshoot, Decimal::new(150, 0));
}

#[test]
fn notional_bar_trade_identity_diagnostics_stay_bounded_and_report_full_membership() {
    let mut bars = NotionalBarBuilder::new(Decimal::new(1000, 0)).unwrap();
    for index in 1..=32 {
        assert!(
            bars.push(trade(&index.to_string(), "10", "1"))
                .unwrap()
                .is_empty()
        );
    }
    let checkpoint = serde_json::to_vec(&bars).unwrap();
    assert!(checkpoint.len() < 4096);
    let completed = bars.push(trade("33", "10", "68")).unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].trade_ids.len(), 16);
    assert_eq!(completed[0].trade_id_count, 33);
    assert!(completed[0].trade_ids_truncated);
    assert_eq!(completed[0].first_trade_id, "1");
    assert_eq!(completed[0].last_trade_id, "33");
    assert_eq!(completed[0].trade_ids_sha256.len(), 64);
}

#[test]
fn duplicate_trade_is_ignored_and_conflicting_identity_invalidates() {
    let mut seen = TradeDeduplicator::new(2).unwrap();
    assert_eq!(
        seen.admit(trade("7", "10", "4")).unwrap(),
        DedupDecision::New
    );
    assert_eq!(
        seen.admit(trade("7", "10", "4")).unwrap(),
        DedupDecision::Duplicate
    );
    assert!(seen.admit(trade("7", "11", "4")).is_err());
}

#[test]
fn trade_count_is_metadata_and_does_not_multiply_notional() {
    let event = parse_trade_payload(
        &json!({"instId":"BTC-USDC","tradeId":"8","px":"10","sz":"4","side":"buy","ts":"100","count":"3"}),
        "okx:spot:BTC-USDC", "trades", 101,
    ).unwrap();
    assert_eq!(event.aggregate_count, 3);
    assert_eq!(event.quote_notional().unwrap(), Decimal::new(40, 0));
}

#[test]
fn future_clock_and_crossed_bbo_are_preserved_as_raw_unqualified_evidence() {
    let bbo = crate::observations::parse_bbo_payload(
        &json!({"ts":"200","bids":[["10.2","0","0",1]],"asks":[["10.1","2","0",1]],"seqId":9}),
        "okx:spot:BTC-USDC",
        "global",
        "meta-1",
        "epoch-1",
        199,
    )
    .unwrap();
    assert!(bbo.reason_codes.contains(&"FUTURE_EVENT_CLOCK".to_string()));
    assert!(bbo.reason_codes.contains(&"INVALID_BBO_STATE".to_string()));
    assert_eq!(bbo.available_at_ms, 199);
    assert_eq!(bbo.bid_quantity, "0");
    assert_eq!(bbo.spread_bps().unwrap(), None);
}

#[test]
fn bbo_parser_accepts_official_single_level_arrays_and_rejects_ticker_or_depth_shapes() {
    let parse = |payload| {
        crate::observations::parse_bbo_payload(
            &payload,
            "okx:spot:BTC-USDC",
            "global",
            "meta-1",
            "epoch-1",
            201,
        )
    };
    let official =
        json!({"ts":"200","bids":[["10.0","2","0",3]],"asks":[["10.2","4","0",5]],"seqId":9});
    let parsed = parse(official).unwrap();
    assert_eq!(parsed.bid_price, "10.0");
    assert_eq!(parsed.bid_quantity, "2");
    assert_eq!(parsed.sequence.as_deref(), Some("9"));
    assert!(parse(json!({"instId":"BTC-USDC","ts":"200","bidPx":"10","bidSz":"2","askPx":"11","askSz":"3","seqId":"9"})).is_err());
    assert!(parse(json!({"ts":"200","bids":[["10","2","0",1],["9","3","0",1]],"asks":[["11","3","0",1]],"seqId":9})).is_err());
    assert!(
        parse(json!({"ts":"200","bids":[["10","2","1",1]],"asks":[["11","3","0",1]],"seqId":9}))
            .is_err()
    );
    assert!(
        parse(json!({"ts":"200","bids":[["10","2","0",1]],"asks":[["11","3","0",1]],"seqId":"9"}))
            .is_err()
    );
    assert!(parse(json!({"instId":"ETH-USDC","ts":"200","bids":[["10","2","0",1]],"asks":[["11","3","0",1]],"seqId":9})).is_err());
}

#[test]
fn dedup_window_loss_keeps_reconnect_overlap_unqualified() {
    let mut seen = TradeDeduplicator::new(1).unwrap();
    assert_eq!(
        seen.admit(trade("1", "10", "1")).unwrap(),
        DedupDecision::New
    );
    seen.begin_epoch();
    assert_eq!(
        seen.admit(trade("2", "10", "1")).unwrap(),
        DedupDecision::New
    );
    assert!(!seen.overlap_verified());
    assert_eq!(
        seen.admit(trade("2", "10", "1")).unwrap(),
        DedupDecision::Duplicate
    );
    assert!(!seen.overlap_verified());
}

#[test]
fn overlap_is_verified_per_instrument_not_by_another_pairs_duplicate() {
    let mut seen = TradeDeduplicator::new(8).unwrap();
    let mut btc = trade("1", "10", "1");
    let mut eth = trade("2", "20", "1");
    eth.instrument_id = "okx:spot:ETH-USDC".into();
    seen.admit(btc.clone()).unwrap();
    seen.admit(eth.clone()).unwrap();
    seen.begin_epoch_for(["okx:spot:BTC-USDC", "okx:spot:ETH-USDC"]);
    assert_eq!(seen.admit(btc.clone()).unwrap(), DedupDecision::Duplicate);
    assert!(seen.overlap_verified_for("okx:spot:BTC-USDC"));
    assert!(!seen.overlap_verified_for("okx:spot:ETH-USDC"));
    btc.trade_id = "3".into();
    assert_eq!(seen.preview(&btc).unwrap(), DedupDecision::New);
}

#[test]
fn dedup_checkpoint_roundtrip_preserves_order_and_overlap_state() {
    let mut seen = TradeDeduplicator::new(2).unwrap();
    seen.admit(trade("1", "10", "1")).unwrap();
    seen.begin_epoch();
    let checkpoint = serde_json::to_vec(&seen.checkpoint()).unwrap();
    let mut restored =
        TradeDeduplicator::restore(serde_json::from_slice(&checkpoint).unwrap()).unwrap();
    assert!(!restored.overlap_verified());
    assert_eq!(
        restored.admit(trade("1", "10", "1")).unwrap(),
        DedupDecision::Duplicate
    );
    assert!(restored.overlap_verified());
}

#[test]
fn replay_checkpoint_restart_matches_uninterrupted_trade_and_timer_transitions() {
    let thresholds = || {
        std::collections::BTreeMap::from([("okx:spot:BTC-USDC".to_string(), Decimal::new(100, 0))])
    };
    let instruments = ["okx:spot:BTC-USDC".to_string()].into_iter().collect();
    let mut uninterrupted = ObservationReplay::new(
        "trades-all",
        "fixture-config",
        &instruments,
        60_000,
        16,
        thresholds(),
        0,
    )
    .unwrap();
    let mut first = trade("1", "10", "2");
    first.quality = "qualified".into();
    first.reason_codes.clear();
    let trade_payload = json!({"record_type":"trade","observation":first});
    uninterrupted.apply(&trade_payload).unwrap();
    let cursor = JournalCursor {
        segment: 0,
        sequence: 1,
        hash: "a".repeat(64),
    };
    let checkpoint = uninterrupted.checkpoint("trades-all", cursor);
    let encoded = serde_json::to_vec(&checkpoint).unwrap();
    let mut restarted =
        ObservationReplay::restore(serde_json::from_slice(&encoded).unwrap(), false).unwrap();

    let second_payload = json!({"record_type":"trade","observation":{
        "schema_version":"okx.observations.v1","venue":"okx","environment":"public","region":"global",
        "instrument_id":"okx:spot:BTC-USDC","channel":"trades-all","trade_id":"2","price":"10",
        "base_quantity":"9","quote_notional":"90","side":"buy","event_at_ms":60_000,
        "available_at_ms":60_001,"aggregate_count":1,"metadata_revision":"meta-1","source_epoch":"epoch-1",
        "connection_id":"conn-1","receive_sequence":2,"raw_sha256":"raw-2","quality":"qualified","reason_codes":[]
    }});
    let expected_notional = uninterrupted.apply(&second_payload).unwrap();
    let actual_notional = restarted.apply(&second_payload).unwrap();
    assert_eq!(
        actual_notional.notional_bars,
        expected_notional.notional_bars
    );
    let timer = json!({"event":"timer","timer_kind":"fixed_time_bar","available_at_ms":120_000,
        "qualified_source":true,"coverage_by_instrument":{"okx:spot:BTC-USDC":{"qualified":true,"coverage_start_ms":0}}});
    let expected = uninterrupted.apply(&timer).unwrap();
    let actual = restarted.apply(&timer).unwrap();
    assert_eq!(actual.time_bars, expected.time_bars);
}

#[test]
fn fixed_time_bars_use_half_open_event_membership() {
    let mut bars = crate::observations::FixedTimeBars::new(60_000).unwrap();
    assert!(
        bars.push(
            ObservationTrade::new(TradeInput {
                instrument_id: "okx:spot:BTC-USDC".into(),
                channel: "trades-all".into(),
                trade_id: "1".into(),
                price: "10".into(),
                base_quantity: "1".into(),
                side: "buy".into(),
                event_at_ms: 59_999,
                available_at_ms: 60_001,
            })
            .unwrap()
        )
        .unwrap()
    );
    assert!(
        bars.push(
            ObservationTrade::new(TradeInput {
                instrument_id: "okx:spot:BTC-USDC".into(),
                channel: "trades-all".into(),
                trade_id: "2".into(),
                price: "11".into(),
                base_quantity: "1".into(),
                side: "buy".into(),
                event_at_ms: 60_000,
                available_at_ms: 60_001,
            })
            .unwrap()
        )
        .unwrap()
    );
    let closed = bars.finalize_through(60_000, true).unwrap();
    assert_eq!(closed.len(), 1);
    assert_eq!((closed[0].start_ms, closed[0].end_ms), (0, 60_000));
    assert_eq!(closed[0].close.as_deref(), Some("10"));
    assert_eq!(
        bars.finalize_through(120_000, true).unwrap()[0].start_ms,
        60_000
    );
    let empty = bars.finalize_through(180_000, true).unwrap();
    assert_eq!(empty.len(), 1);
    assert_eq!(empty[0].quality, "qualified_empty");
    assert_eq!(empty[0].open, None);
    assert_eq!(empty[0].trade_count, 0);
}

#[test]
fn fixed_time_bars_do_not_emit_zero_activity_for_an_outage() {
    let mut bars = crate::observations::FixedTimeBars::new_at(60_000, 60_000).unwrap();
    bars.reset_at(180_000);
    let outage = bars.finalize_through(240_000, false).unwrap();
    assert_eq!(outage.len(), 1);
    assert_eq!(outage[0].quality, "unavailable");
    assert_eq!(outage[0].open, None);
    assert_eq!(outage[0].reason_codes, ["SOURCE_COVERAGE_UNVERIFIED"]);
    let healthy_quiet = bars.finalize_through(300_000, true).unwrap();
    assert_eq!(healthy_quiet.len(), 1);
    assert_eq!(healthy_quiet[0].quality, "qualified_empty");
}

#[test]
fn invalid_trade_timestamps_cannot_grow_time_bar_checkpoint_state() {
    let mut bars = crate::observations::FixedTimeBars::new_at(60_000, 60_000).unwrap();
    for minute in 0..10_000u64 {
        bars.invalidate_event_time(minute * 60_000, "UNQUALIFIED_TRADE");
        bars.invalidate_event_time((1_000_000 + minute) * 60_000, "UNQUALIFIED_TRADE");
    }
    bars.invalidate_event_time(120_000, "UNQUALIFIED_TRADE");
    let checkpoint = serde_json::to_value(&bars).unwrap();
    assert!(
        checkpoint["unavailable_intervals"]
            .as_object()
            .unwrap()
            .len()
            <= 3
    );
    assert!(checkpoint["unavailable_intervals"].get("120000").is_some());
}

#[test]
fn unqualified_trade_invalidates_its_interval_instead_of_creating_a_quiet_zero() {
    let mut bars = crate::observations::FixedTimeBars::new(60_000).unwrap();
    bars.invalidate_event_time(30_000, "UNQUALIFIED_TRADE");
    let result = bars.finalize_through(60_000, true).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].quality, "unavailable");
    assert_eq!(result[0].reason_codes, ["UNQUALIFIED_TRADE"]);
    assert_eq!(result[0].trade_count, 0);
}

#[test]
fn fixed_time_bar_timer_discontinuity_is_bounded_and_does_not_advance_state() {
    let mut bars = crate::observations::FixedTimeBars::new_at(60_000, 60_000).unwrap();
    assert!(
        bars.finalize_through(365 * 24 * 60 * 60 * 1000, true)
            .is_err()
    );
    assert_eq!(bars.last_finalized_end_ms(), 60_000);
    assert!(bars.finalize_through(59_999, true).is_err());
    assert_eq!(bars.last_finalized_end_ms(), 60_000);
}

#[test]
fn durable_gap_before_discontinuous_timer_replays_as_a_new_feature_epoch() {
    let instruments = std::collections::BTreeSet::from(["okx:spot:BTC-USDC".to_string()]);
    let mut replay = ObservationReplay::new(
        "trades-all",
        "identity",
        &instruments,
        60_000,
        8,
        std::collections::BTreeMap::new(),
        60_000,
    )
    .unwrap();
    replay
        .apply(&json!({"event":"gap","channel":"trades-all","instrument_id":"okx:spot:BTC-USDC","available_at_ms":3_600_000}))
        .unwrap();
    let discontinuity = replay
        .apply(&json!({"schema_version":"okx.observations.timer.v1","event":"timer",
            "timer_kind":"fixed_time_bar","available_at_ms":3_600_000,"qualified_source":false,
            "coverage_by_instrument":{"okx:spot:BTC-USDC":{"qualified":false,"coverage_start_ms":60_000}}}))
        .unwrap();
    assert!(discontinuity.time_bars.is_empty());
    let healthy_quiet = replay
        .apply(&json!({"schema_version":"okx.observations.timer.v1","event":"timer",
            "timer_kind":"fixed_time_bar","available_at_ms":3_660_000,"qualified_source":true,
            "coverage_by_instrument":{"okx:spot:BTC-USDC":{"qualified":true,"coverage_start_ms":60_000}}}))
        .unwrap();
    assert_eq!(healthy_quiet.time_bars.len(), 1);
    assert_eq!(healthy_quiet.time_bars[0].1.quality, "qualified_empty");
}

#[test]
fn fresh_trade_epoch_accepts_distinct_ids_without_requiring_replay_overlap() {
    let mut dedup = TradeDeduplicator::new(100).unwrap();
    assert!(!dedup.has_history_for("okx:spot:BTC-USDC"));
    assert!(dedup.overlap_verified_for("okx:spot:BTC-USDC"));
    for id in ["1001", "1002"] {
        let trade = ObservationTrade::new(TradeInput {
            instrument_id: "okx:spot:BTC-USDC".into(),
            channel: "trades-all".into(),
            trade_id: id.into(),
            price: "10".into(),
            base_quantity: "2".into(),
            side: "buy".into(),
            event_at_ms: 30_000,
            available_at_ms: 30_001,
        })
        .unwrap();
        assert_eq!(dedup.admit(trade).unwrap(), DedupDecision::New);
    }
    assert!(dedup.overlap_verified_for("okx:spot:BTC-USDC"));
    assert_eq!(dedup.checkpoint().entries.len(), 2);
}

#[test]
fn reconnect_accepts_new_epoch_prints_without_duplicate_overlap() {
    let mut dedup = TradeDeduplicator::new(100).unwrap();
    let old = ObservationTrade::new(TradeInput {
        instrument_id: "okx:spot:BTC-USDC".into(),
        channel: "trades-all".into(),
        trade_id: "1000".into(),
        price: "10".into(),
        base_quantity: "5".into(),
        side: "buy".into(),
        event_at_ms: 5_000,
        available_at_ms: 5_001,
    })
    .unwrap();
    dedup.admit(old).unwrap();
    dedup.begin_epoch_for(["okx:spot:BTC-USDC"]);
    assert!(!dedup.overlap_verified_for("okx:spot:BTC-USDC"));

    let new_epoch_trade = ObservationTrade::new(TradeInput {
        instrument_id: "okx:spot:BTC-USDC".into(),
        channel: "trades-all".into(),
        trade_id: "1001".into(),
        price: "11".into(),
        base_quantity: "3".into(),
        side: "buy".into(),
        event_at_ms: 65_000,
        available_at_ms: 65_001,
    })
    .unwrap();
    assert_eq!(
        dedup.admit(new_epoch_trade.clone()).unwrap(),
        DedupDecision::New
    );
    let mut bars = crate::observations::FixedTimeBars::new_at(60_000, 60_000).unwrap();
    bars.push(new_epoch_trade).unwrap();
    let rebuilt = bars.finalize_through(120_000, true).unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(rebuilt[0].quality, "qualified");
    assert_eq!(rebuilt[0].quote_notional.to_string(), "33");
}

#[test]
fn evicted_trade_ids_remain_quarantined_until_event_time_passes_the_checkpointed_floor() {
    let mut dedup = TradeDeduplicator::new(1).unwrap();
    for (id, event_at_ms) in [("1", 60_000), ("2", 120_000), ("3", 180_000)] {
        dedup
            .admit(
                ObservationTrade::new(TradeInput {
                    instrument_id: "okx:spot:BTC-USDC".into(),
                    channel: "trades-all".into(),
                    trade_id: id.into(),
                    price: "10".into(),
                    base_quantity: "1".into(),
                    side: "buy".into(),
                    event_at_ms,
                    available_at_ms: event_at_ms + 1,
                })
                .unwrap(),
            )
            .unwrap();
    }
    assert!(!dedup.event_is_after_retention_floor("okx:spot:BTC-USDC", 120_000));
    assert!(dedup.event_is_after_retention_floor("okx:spot:BTC-USDC", 120_001));
    let checkpoint = dedup.checkpoint();
    let restored = TradeDeduplicator::restore(checkpoint).unwrap();
    assert!(!restored.event_is_after_retention_floor("okx:spot:BTC-USDC", 120_000));
    assert!(restored.event_is_after_retention_floor("okx:spot:BTC-USDC", 120_001));
}

#[test]
fn maximum_dedup_checkpoint_and_300_pair_builder_inventory_fit_the_shared_row_bound() {
    let mut dedup = TradeDeduplicator::new(8_192).unwrap();
    for index in 0..8_192 {
        dedup
            .admit(
                ObservationTrade::new(TradeInput {
                    instrument_id: "okx:spot:BTC-USDC".into(),
                    channel: "trades-all".into(),
                    trade_id: index.to_string(),
                    price: "1234.5678".into(),
                    base_quantity: "98765.4321".into(),
                    side: "sell".into(),
                    event_at_ms: index as u64 + 1,
                    available_at_ms: index as u64 + 1,
                })
                .unwrap(),
            )
            .unwrap();
    }
    let time_bars = (0..300)
        .map(|index| {
            let instrument = format!("okx:spot:BASE{index}-USDC");
            (
                instrument,
                crate::observations::FixedTimeBars::new(60_000).unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let notional_bars = (0..300)
        .map(|index| {
            let instrument = format!("okx:spot:BASE{index}-USDC");
            (
                instrument,
                NotionalBarBuilder::new(Decimal::new(100, 0)).unwrap(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let payload = json!({"record_type":"checkpoint","schema_version":"okx.observations.record.v1",
        "checkpoint":{"schema_version":"okx.observations.replay-checkpoint.v1",
        "dedup":dedup.checkpoint(),"time_bars":time_bars,"notional_bars":notional_bars}});
    let serialized = serde_json::to_vec(&payload).unwrap();
    assert!(serialized.len() + 256 < crate::observation_journal::MAX_JOURNAL_ROW_BYTES);
}
