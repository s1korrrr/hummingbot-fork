//! Write a synthetic Rust-produced tape for offline cross-owner contract verification.

use anyhow::{Context, Result, ensure};
use okx_candle_service::observation_config::InstrumentMetadata;
use okx_candle_service::observation_journal::{
    ObservationJournal, TapeSealInput, TapeStreamReceipt,
};
use okx_candle_service::observation_metadata::decode_metadata;
use okx_candle_service::observations::{
    FixedTimeBars, NotionalBarBuilder, TradeDeduplicator, parse_bbo_payload, parse_trade_payload,
    payload_sha256,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

fn main() -> Result<()> {
    let output = std::env::args_os()
        .nth(1)
        .context("usage: observation_tape_fixture OUTPUT_DIR")?;
    let output = PathBuf::from(output);
    fs::create_dir(&output).context("output directory must not already exist")?;
    fs::set_permissions(&output, fs::Permissions::from_mode(0o700))?;
    let mut journal = ObservationJournal::open(&output, 64 * 1024)?;
    let ack = |channel: &str, epoch: &str, sequence: u64| -> Result<Value> {
        let raw_payload =
            json!({"event":"subscribe","arg":{"channel":channel,"instId":"BTC-USDC"}});
        let raw_sha256 = payload_sha256(serde_json::to_string(&raw_payload)?.as_bytes());
        Ok(
            json!({"schema_version":"okx.observations.source-event.v1","event":"subscription_ack",
            "environment":"public","region":"global","source_epoch":epoch,
            "connection_id":"fixture-connection","channel":channel,"inst_id":"BTC-USDC",
            "available_at_ms":100,"receive_sequence":sequence,"raw_sha256":raw_sha256,"raw_payload":raw_payload}),
        )
    };
    journal.append(ack("trades-all", "fixture-trades-epoch", 1)?)?;
    journal.append(ack("bbo-tbt", "fixture-bbo-epoch", 2)?)?;
    let metadata_raw = br#"{"code":"0","data":[{"instId":"BTC-USDC","instType":"SPOT","quoteCcy":"USDC","state":"live","tickSz":"0.1","lotSz":"0.01","minSz":"0.01","listTime":"10","contTdSwTime":"","ruleType":"normal","upcChg":[]}]}"#;
    let metadata_available_at_ms = 99;
    let metadata = decode_metadata(
        metadata_raw,
        &[InstrumentMetadata {
            inst_id: "BTC-USDC".into(),
            instrument_id: "okx:spot:BTC-USDC".into(),
            metadata_revision: "fixture-meta-v1".into(),
            tick_size: "0.1".into(),
            lot_size: "0.01".into(),
            minimum_size: "0.01".into(),
            effective_at_ms: 1,
            available_at_ms: 1,
        }],
        metadata_available_at_ms,
    )?;
    journal.append(json!({"schema_version":"okx.observations.raw-metadata.v1","event":"metadata_response",
        "channel":"metadata","venue":"okx","environment":"public","region":"global","http_status":200,
        "available_at_ms":metadata_available_at_ms,"raw_sha256":metadata.raw_sha256,"raw_text":metadata.raw_text}))?;
    for observation in metadata.observations {
        journal.append(observation)?;
    }
    let trade_wire = json!({"instId":"BTC-USDC","tradeId":"1","px":"10","sz":"2","side":"buy","ts":"100","count":"1"});
    let trade_frame =
        json!({"arg":{"channel":"trades-all","instId":"BTC-USDC"},"data":[trade_wire]});
    let trade_text = serde_json::to_string(&trade_frame)?;
    let trade_hash = payload_sha256(trade_text.as_bytes());
    journal.append(json!({"schema_version":"okx.observations.raw-frame.v1","event":"raw_frame","environment":"public",
        "region":"global","source_epoch":"fixture-trades-epoch","connection_id":"fixture-connection","channel":"trades-all",
        "inst_id":"BTC-USDC","receive_sequence":3,"receive_utc_ns":101000000,"receive_monotonic_ns":1000000,
        "clock_epoch":"fixture-clock","available_at_ms":101,"raw_sha256":trade_hash,"raw_text":trade_text}))?;
    let mut trade = parse_trade_payload(&trade_wire, "okx:spot:BTC-USDC", "trades-all", 101)?;
    trade.environment = "public".into();
    trade.region = "global".into();
    trade.metadata_revision = "fixture-meta-v1".into();
    trade.source_epoch = "fixture-trades-epoch".into();
    trade.connection_id = "fixture-connection".into();
    trade.receive_sequence = 3;
    trade.raw_sha256 = trade_hash.clone();
    trade.quality = "qualified".into();
    trade.reason_codes.clear();
    let trade_cursor = journal.append_cursor(json!({"record_type":"trade","schema_version":"okx.observations.record.v1","venue":"okx",
        "environment":"public","region":"global","instrument_id":"okx:spot:BTC-USDC","channel":"trades-all",
        "source_epoch":"fixture-trades-epoch","connection_id":"fixture-connection","receive_sequence":3,
        "receive_utc_ns":101000000,"receive_monotonic_ns":1000000,"clock_epoch":"fixture-clock",
        "available_at_ms":101,"metadata_revision":"fixture-meta-v1","raw_sha256":trade_hash,
        "raw_frame_sequence":3,"observation":trade}))?;

    // A duplicate is retained as an explicit disposition row, not silently dropped.
    let duplicate_raw = serde_json::to_string(&trade_frame)?;
    let duplicate_hash = payload_sha256(duplicate_raw.as_bytes());
    journal.append(json!({"schema_version":"okx.observations.raw-frame.v1","event":"raw_frame",
        "environment":"public","region":"global","source_epoch":"fixture-trades-epoch",
        "connection_id":"fixture-connection","channel":"trades-all","inst_id":"BTC-USDC",
        "receive_sequence":5,"available_at_ms":102,"raw_sha256":duplicate_hash,"raw_text":duplicate_raw}))?;
    journal.append(
        json!({"schema_version":"okx.observations.dedup.v1","event":"duplicate",
        "instrument_id":"okx:spot:BTC-USDC","channel":"trades-all","trade_id":"1",
        "source_epoch":"fixture-trades-epoch","raw_frame_sequence":5,"available_at_ms":102}),
    )?;

    // Invalid source bytes are preserved with a matching rejection disposition.
    let rejected_raw = "{";
    let rejected_hash = payload_sha256(rejected_raw.as_bytes());
    journal.append(
        json!({"schema_version":"okx.observations.raw-frame.v1","event":"raw_frame",
        "environment":"public","region":"global","source_epoch":"fixture-trades-epoch",
        "connection_id":"fixture-connection","channel":"trades-all","receive_sequence":6,
        "available_at_ms":103,"raw_sha256":rejected_hash,"raw_text":rejected_raw}),
    )?;
    journal.append(
        json!({"schema_version":"okx.observations.rejection.v1","event":"parse_rejection",
        "channel":"trades-all","source_epoch":"fixture-trades-epoch","raw_frame_sequence":6,
        "raw_sha256":rejected_hash,"reason":"INVALID_JSON","available_at_ms":103}),
    )?;

    // These deterministic derived rows exercise the exact capture wrapper schema.
    journal.append(
        json!({"schema_version":"okx.observations.timer.v1","event":"timer",
        "timer_kind":"fixed_time_bar","duration_ms":60_000,"source_epoch":"fixture-trades-epoch",
        "channel":"trades-all","available_at_ms":60_000,"qualified_source":true,
        "coverage_by_instrument":{"okx:spot:BTC-USDC":{"qualified":true,"coverage_start_ms":0}}}),
    )?;
    let mut time_builder = FixedTimeBars::new_at(60_000, 0)?;
    time_builder.push(trade.clone())?;
    let time_bar = time_builder
        .finalize_through(60_000, true)?
        .into_iter()
        .next()
        .context("synthetic time bar did not close")?;
    let time_cursor = journal.append_cursor(json!({"record_type":"time_bar",
        "schema_version":"okx.observations.record.v1","source_epoch":"fixture-trades-epoch",
        "instrument_id":"okx:spot:BTC-USDC","channel":"trades-all",
        "published_at_ms":60_000,"bar":time_bar}))?;

    let mut notional_builder = NotionalBarBuilder::new(Decimal::new(10, 0))?;
    let notional_bar = notional_builder
        .push(trade.clone())?
        .into_iter()
        .next()
        .context("synthetic notional bar did not close")?;
    journal.append(
        json!({"record_type":"notional_bar","schema_version":"okx.observations.record.v1",
        "source_epoch":"fixture-trades-epoch","instrument_id":"okx:spot:BTC-USDC","channel":"trades-all",
        "available_at_ms":102,"published_at_ms":102,"close_cursor":trade_cursor,
        "bar":notional_bar}),
    )?;

    let mut dedup = TradeDeduplicator::new(16)?;
    dedup.admit(trade)?;
    let mut time_bars = std::collections::BTreeMap::new();
    time_bars.insert("okx:spot:BTC-USDC".to_string(), time_builder);
    let mut notional_bars = std::collections::BTreeMap::new();
    notional_bars.insert("okx:spot:BTC-USDC".to_string(), notional_builder);
    let checkpoint = okx_candle_service::observations::ReplayCheckpoint {
        schema_version: "okx.observations.replay-checkpoint.v1".into(),
        channel: "trades-all".into(),
        cursor: time_cursor,
        bar_duration_ms: 60_000,
        configuration_identity: "synthetic-config-identity".into(),
        dedup: dedup.checkpoint(),
        time_bars,
        notional_bars,
    };
    journal.append(
        json!({"record_type":"checkpoint","schema_version":"okx.observations.record.v1",
        "source_epoch":"fixture-trades-epoch","channel":"trades-all","available_at_ms":60_001,
        "checkpoint":checkpoint}),
    )?;

    let bbo_wires = [100u64, 150, 199].map(|timestamp| {
        json!({
            "ts":timestamp.to_string(),"bids":[["9.9","4","0",1]],
            "asks":[["10.1","5","0",1]],"seqId":timestamp
        })
    });
    let bbo_frame = json!({"arg":{"channel":"bbo-tbt","instId":"BTC-USDC"},"data":bbo_wires});
    let bbo_text = serde_json::to_string(&bbo_frame)?;
    let bbo_hash = payload_sha256(bbo_text.as_bytes());
    journal.append(json!({"schema_version":"okx.observations.raw-frame.v1","event":"raw_frame","environment":"public",
        "region":"global","source_epoch":"fixture-bbo-epoch","connection_id":"fixture-bbo-connection","channel":"bbo-tbt",
        "inst_id":"BTC-USDC","receive_sequence":4,"receive_utc_ns":101000000,"receive_monotonic_ns":1000001,
        "clock_epoch":"fixture-clock","available_at_ms":101,"raw_sha256":bbo_hash,"raw_text":bbo_text}))?;
    for bbo_wire in &bbo_wires {
        let event_at_ms = bbo_wire["ts"].as_str().unwrap().parse::<u64>()?;
        let available_at_ms = event_at_ms + 1;
        let mut bbo = parse_bbo_payload(
            bbo_wire,
            "okx:spot:BTC-USDC",
            "global",
            "fixture-meta-v1",
            "fixture-bbo-epoch",
            available_at_ms,
        )?;
        bbo.quality = "qualified".into();
        journal.append(json!({"record_type":"bbo","schema_version":"okx.observations.record.v1","venue":"okx",
            "environment":"public","region":"global","instrument_id":"okx:spot:BTC-USDC","channel":"bbo-tbt",
            "source_epoch":"fixture-bbo-epoch","connection_id":"fixture-bbo-connection","receive_sequence":4,
            "receive_utc_ns":available_at_ms * 1_000_000,"receive_monotonic_ns":1_000_001,
            "clock_epoch":"fixture-clock","available_at_ms":available_at_ms,"metadata_revision":"fixture-meta-v1",
            "raw_sha256":bbo_hash,"raw_frame_sequence":4,"observation":bbo}))?;
    }
    let receipt = |channel: &str| TapeStreamReceipt {
        channel: channel.into(),
        instrument_id: "okx:spot:BTC-USDC".into(),
        coverage_start_ms: 100,
        coverage_end_ms: 200,
        record_count: if channel == "bbo-tbt" { 3 } else { 1 },
        gap_count: 0,
        subscription_acknowledged: true,
        metadata_revision: "fixture-meta-v1".into(),
        receipt_source: "capture_report".into(),
    };
    let manifest = journal.seal_tape(TapeSealInput {
        environment: "public".into(),
        region: "global".into(),
        source_profile: "okx_global_public_v1".into(),
        producer_revision: "synthetic-fixture".into(),
        capture_session: "offline-rust-contract-fixture".into(),
        availability_semantics: "source_receipt_only_publication_unavailable".into(),
        scored_start_ms: 100,
        scored_end_ms: 200,
        sealed_at_ms: 201,
        streams: vec![
            receipt("trades-all"),
            receipt("bbo-tbt"),
            receipt("metadata"),
        ],
    })?;
    ensure!(
        manifest.exists(),
        "sealed tape manifest missing after write"
    );
    println!("{}", manifest.display());
    Ok(())
}
