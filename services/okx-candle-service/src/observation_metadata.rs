//! Public metadata observation. Configured values are expectations, never exchange receipts.
use crate::observation_config::{InstrumentMetadata, Region};
use crate::observations::{payload_sha256, validate_positive_decimal};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct MetadataReceipt {
    pub raw_sha256: String,
    pub raw_text: String,
    pub available_at_ms: u64,
    pub observations: Vec<Value>,
}

/// Validate only the frozen universe. A changed rule or non-live listing stops
/// qualification; it cannot silently change a partially built bar's units.
pub fn decode_metadata(
    raw: &[u8],
    expected: &[InstrumentMetadata],
    available_at_ms: u64,
) -> Result<MetadataReceipt> {
    ensure!(
        !raw.is_empty() && raw.len() <= MAX_RESPONSE_BYTES,
        "metadata response exceeds byte bound"
    );
    ensure!(
        (1..=300).contains(&expected.len()),
        "bounded metadata universe required"
    );
    let body: Value = serde_json::from_slice(raw).context("invalid metadata JSON")?;
    ensure!(
        body["code"] == "0",
        "metadata endpoint returned non-success code"
    );
    let rows = body["data"]
        .as_array()
        .context("metadata data array missing")?;
    ensure!(
        rows.len() <= 10_000,
        "metadata catalog exceeds record bound"
    );
    let mut selected = BTreeMap::new();
    for row in rows {
        let id = row["instId"]
            .as_str()
            .context("metadata instrument identity missing")?;
        if expected.iter().any(|item| item.inst_id == id) {
            ensure!(
                selected.insert(id, row).is_none(),
                "duplicate metadata instrument"
            );
        }
    }
    let raw_sha256 = payload_sha256(raw);
    let mut observations = Vec::with_capacity(expected.len());
    for item in expected {
        let row = selected
            .get(item.inst_id.as_str())
            .context("configured instrument absent from exchange catalog")?;
        ensure!(
            row["instType"] == "SPOT" && row["quoteCcy"] == "USDC",
            "exchange metadata is not USDC spot"
        );
        ensure!(row["state"] == "live", "configured instrument is not live");
        for (field, configured) in [
            ("tickSz", &item.tick_size),
            ("lotSz", &item.lot_size),
            ("minSz", &item.minimum_size),
        ] {
            let observed = row[field]
                .as_str()
                .context("exchange metadata rule missing")?;
            ensure!(
                validate_positive_decimal(observed, field)?
                    == validate_positive_decimal(configured, field)?,
                "exchange metadata differs from frozen instrument rules: {} {}",
                item.inst_id,
                field
            );
        }
        let list_time = row["listTime"]
            .as_str()
            .context("listing timestamp missing")?
            .parse::<u64>()
            .context("invalid listing timestamp")?;
        let continuous_time = match row["contTdSwTime"]
            .as_str()
            .context("continuous trading timestamp missing")?
        {
            "" => list_time,
            value => value
                .parse::<u64>()
                .context("invalid continuous trading timestamp")?,
        };
        ensure!(
            list_time <= available_at_ms && continuous_time <= available_at_ms,
            "instrument continuous trading begins in future"
        );
        // Upcoming rule changes cannot be ignored merely because today's sizes
        // match. A new frozen catalog/epoch is required before their activation.
        ensure!(
            row.get("ruleType").and_then(Value::as_str) == Some("normal"),
            "unsupported exchange instrument rule type"
        );
        ensure!(
            row.get("upcChg")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty),
            "pending exchange metadata changes require a new frozen catalog"
        );
        let projection = json!({"instId":item.inst_id,"instType":"SPOT","state":"live","quoteCcy":"USDC",
            "tickSz":row["tickSz"],"lotSz":row["lotSz"],"minSz":row["minSz"],
            "listTime":list_time,"continuous_trading_start_ms":continuous_time});
        observations.push(json!({"schema_version":"okx.observations.metadata.v1", "event":"exchange_metadata_observed",
            "instrument_id":item.instrument_id,"channel":"metadata","metadata_revision":item.metadata_revision,
            "observed_revision_sha256":payload_sha256(&serde_json::to_vec(&projection)?),
            "available_at_ms":available_at_ms,"effective_at_ms":null,
            "effective_time_semantics":"exchange_rule_effective_time_not_reported",
            "raw_sha256":raw_sha256,"metadata":projection}));
    }
    Ok(MetadataReceipt {
        raw_sha256,
        raw_text: std::str::from_utf8(raw)?.into(),
        available_at_ms,
        observations,
    })
}

/// Called only by explicitly enabled observation capture. No credentials or URL
/// override; redirects are disabled so regional identity remains explicit.
pub async fn fetch_metadata(region: Region) -> Result<(Vec<u8>, u64, u16)> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?;
    let response = client
        .get(format!("{}/api/v5/public/instruments", region.rest_url()))
        .query(&[("instType", "SPOT")])
        .send()
        .await?;
    let status = response.status().as_u16();
    if let Some(length) = response.content_length() {
        ensure!(
            length <= MAX_RESPONSE_BYTES as u64,
            "metadata response exceeds byte bound"
        );
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= MAX_RESPONSE_BYTES,
            "metadata response exceeds byte bound"
        );
        bytes.extend_from_slice(&chunk);
    }
    let available_at_ms = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    Ok((bytes, available_at_ms, status))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected() -> Vec<InstrumentMetadata> {
        vec![InstrumentMetadata {
            inst_id: "BTC-USDC".into(),
            instrument_id: "okx:spot:BTC-USDC".into(),
            metadata_revision: "frozen-catalog".into(),
            tick_size: "0.1".into(),
            lot_size: "0.001".into(),
            minimum_size: "0.01".into(),
            effective_at_ms: 1,
            available_at_ms: 2,
        }]
    }
    fn catalog() -> Value {
        json!({"code":"0","data":[{"instId":"BTC-USDC","instType":"SPOT","quoteCcy":"USDC",
            "state":"live","tickSz":"0.10","lotSz":"0.001","minSz":"0.01","listTime":"10","contTdSwTime":"",
            "ruleType":"normal","upcChg":[]}]})
    }
    #[test]
    fn observed_metadata_preserves_unknown_rule_effective_time() {
        let receipt =
            decode_metadata(&serde_json::to_vec(&catalog()).unwrap(), &expected(), 100).unwrap();
        assert_eq!(receipt.observations[0]["available_at_ms"], 100);
        assert!(receipt.observations[0]["effective_at_ms"].is_null());
        assert_eq!(
            receipt.observations[0]["metadata_revision"],
            "frozen-catalog"
        );
        assert_eq!(receipt.raw_sha256.len(), 64);
    }
    #[test]
    fn changed_rules_listing_future_and_pending_rules_fail_closed() {
        for (field, value) in [
            ("tickSz", json!("0.2")),
            ("state", json!("suspend")),
            ("listTime", json!("101")),
            ("ruleType", json!("unsupported")),
            (
                "upcChg",
                json!([{"param":"tickSz","newValue":"0.2","effTime":"200"}]),
            ),
        ] {
            let mut body = catalog();
            body["data"][0][field] = value;
            assert!(
                decode_metadata(&serde_json::to_vec(&body).unwrap(), &expected(), 100).is_err()
            );
        }
    }
    #[test]
    fn missing_duplicate_or_failed_catalog_is_not_metadata() {
        let mut duplicate = catalog();
        let repeated = duplicate["data"][0].clone();
        duplicate["data"].as_array_mut().unwrap().push(repeated);
        for body in [
            json!({"code":"0","data":[]}),
            json!({"code":"50011","data":[]}),
            duplicate,
        ] {
            assert!(
                decode_metadata(&serde_json::to_vec(&body).unwrap(), &expected(), 100).is_err()
            );
        }
    }
}
