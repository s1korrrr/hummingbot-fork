//! Explicit, bounded opt-in configuration for public observation capture.

use anyhow::{Result, ensure};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::{collections::BTreeSet, fs, path::Path};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    Global,
    Eea,
}

impl Region {
    pub fn public_ws_url(self) -> &'static str {
        match self {
            Self::Global => "wss://ws.okx.com/ws/v5/public",
            Self::Eea => "wss://wseea.okx.com/ws/v5/public",
        }
    }
    pub fn business_ws_url(self) -> &'static str {
        match self {
            Self::Global => "wss://ws.okx.com/ws/v5/business",
            Self::Eea => "wss://wseea.okx.com/ws/v5/business",
        }
    }
    pub fn rest_url(self) -> &'static str {
        match self {
            Self::Global => "https://www.okx.com",
            Self::Eea => "https://eea.okx.com",
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::Eea => "eea",
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationSettings {
    pub schema_version: String,
    pub enabled: bool,
    pub environment: String,
    pub region: Region,
    pub instruments: Vec<InstrumentMetadata>,
    pub notional_bars: Vec<NotionalBarCalibration>,
    pub channels: Vec<String>,
    pub limits: ObservationLimits,
    pub journal_dir: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotionalBarCalibration {
    pub instrument_id: String,
    pub quote_threshold: String,
    pub calibration_id: String,
    pub calibration_receipt_sha256: String,
    pub estimator_version: String,
    pub calibration_start_ms: u64,
    pub calibration_end_ms: u64,
    pub scored_start_ms: u64,
    pub target_duration_seconds: u64,
    pub median_valid_60s_turnover_quote: String,
    pub valid_intervals: usize,
    pub minimum_valid_intervals: usize,
    pub valid_coverage_ratio: String,
    pub minimum_coverage_ratio: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentMetadata {
    pub inst_id: String,
    pub instrument_id: String,
    pub metadata_revision: String,
    pub tick_size: String,
    pub lot_size: String,
    pub minimum_size: String,
    pub effective_at_ms: u64,
    pub available_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationLimits {
    pub max_pairs: usize,
    pub max_sockets: usize,
    pub max_subscriptions: usize,
    pub max_frame_bytes: usize,
    pub queue_capacity: usize,
    pub dedup_identities: usize,
    pub max_replay_records: usize,
    pub metadata_refresh_interval_ms: u64,
    pub segment_bytes: u64,
    pub minimum_disk_free_bytes: u64,
    pub raw_retention_days: u32,
}

impl ObservationSettings {
    pub fn observation_identity(&self) -> Result<String> {
        let instruments = self
            .instruments
            .iter()
            .map(|item| {
                serde_json::json!({
                    "inst_id":item.inst_id,"instrument_id":item.instrument_id,
                    "metadata_revision":item.metadata_revision,"tick_size":item.tick_size,
                    "lot_size":item.lot_size,"minimum_size":item.minimum_size,
                    "effective_at_ms":item.effective_at_ms,"available_at_ms":item.available_at_ms
                })
            })
            .collect::<Vec<_>>();
        let calibrations = self.notional_bars.iter().map(|item| serde_json::json!({
            "instrument_id":item.instrument_id,"quote_threshold":item.quote_threshold,
            "calibration_id":item.calibration_id,"calibration_receipt_sha256":item.calibration_receipt_sha256,
            "estimator_version":item.estimator_version,"calibration_start_ms":item.calibration_start_ms,
            "calibration_end_ms":item.calibration_end_ms,"scored_start_ms":item.scored_start_ms,
            "target_duration_seconds":item.target_duration_seconds,
            "median_valid_60s_turnover_quote":item.median_valid_60s_turnover_quote,
            "valid_intervals":item.valid_intervals,"minimum_valid_intervals":item.minimum_valid_intervals,
            "valid_coverage_ratio":item.valid_coverage_ratio,"minimum_coverage_ratio":item.minimum_coverage_ratio
        })).collect::<Vec<_>>();
        let value = serde_json::json!({
            "schema_version":self.schema_version,"environment":self.environment,
            "region":self.region.as_str(),"source_profile":format!("okx_{}_public_v1",self.region.as_str()),
            "instruments":instruments,"channels":self.channels,"notional_bars":calibrations,
            "metadata_refresh_interval_ms":self.limits.metadata_refresh_interval_ms
        });
        Ok(crate::observations::payload_sha256(&serde_json::to_vec(
            &value,
        )?))
    }

    pub fn read(path: impl AsRef<Path>, service_pairs: &[String]) -> Result<Self> {
        let path = path.as_ref();
        let meta = fs::symlink_metadata(path)?;
        ensure!(
            meta.is_file() && meta.len() <= 64_000,
            "observation settings must be a regular file <=64KB"
        );
        let settings: Self = serde_json::from_slice(&fs::read(path)?)?;
        settings.validate(service_pairs)?;
        Ok(settings)
    }

    pub fn validate(&self, service_pairs: &[String]) -> Result<()> {
        ensure!(
            self.schema_version == "okx.observations.config.v1",
            "unsupported observation config schema"
        );
        ensure!(
            self.enabled,
            "an observation config file must explicitly enable capture"
        );
        ensure!(
            self.environment == "public",
            "only public market-data capture is supported"
        );
        ensure!(
            !self.instruments.is_empty() && self.instruments.len() <= self.limits.max_pairs,
            "observation instrument count exceeds max_pairs"
        );
        ensure!(
            (1..=300).contains(&self.limits.max_pairs),
            "max_pairs must be 1..300"
        );
        ensure!(
            (1..=16).contains(&self.limits.max_sockets),
            "max_sockets must be 1..16"
        );
        ensure!(
            (1..=600).contains(&self.limits.max_subscriptions),
            "max_subscriptions must be 1..600"
        );
        ensure!(
            (1..=1_048_576).contains(&self.limits.max_frame_bytes),
            "max_frame_bytes must be 1..1048576"
        );
        ensure!(
            (1..=65_536).contains(&self.limits.queue_capacity),
            "queue_capacity must be 1..65536"
        );
        ensure!(
            (1..=8_192).contains(&self.limits.dedup_identities),
            "dedup_identities must be 1..8192 so checkpoints stay within the shared row bound"
        );
        ensure!(
            (1..=1_000_000).contains(&self.limits.max_replay_records),
            "max_replay_records must be 1..1000000"
        );
        ensure!(
            (16 * 1024 * 1024..=1_073_741_824).contains(&self.limits.segment_bytes),
            "segment_bytes must be 16 MiB..1 GiB so the bounded metadata response can be journaled"
        );
        ensure!(
            (1_000..=300_000).contains(&self.limits.metadata_refresh_interval_ms),
            "metadata_refresh_interval_ms must be 1000..300000"
        );
        ensure!(
            self.limits.minimum_disk_free_bytes > 0,
            "minimum disk floor is required"
        );
        ensure!(
            (1..=3650).contains(&self.limits.raw_retention_days),
            "raw retention must be 1..3650 days"
        );
        ensure!(
            !self.journal_dir.is_empty() && self.journal_dir.len() <= 4096,
            "journal_dir is required"
        );
        ensure!(
            !self.channels.is_empty(),
            "at least one observation channel is required"
        );
        ensure!(
            self.instruments.len().saturating_mul(self.channels.len())
                <= self.limits.max_subscriptions,
            "observation subscription count exceeds configured limit"
        );
        let channel_set: BTreeSet<_> = self.channels.iter().collect();
        ensure!(
            channel_set.len() == self.channels.len(),
            "duplicate observation channel"
        );
        ensure!(
            self.channels
                .iter()
                .all(|c| matches!(c.as_str(), "trades-all" | "bbo-tbt")),
            "only trades-all and bbo-tbt are supported"
        );
        ensure!(
            self.limits.max_sockets >= self.channels.len(),
            "max_sockets must cover one independent socket per channel"
        );
        let mut ids = BTreeSet::new();
        let mut canonical_ids = BTreeSet::new();
        let service: BTreeSet<_> = service_pairs.iter().collect();
        for instrument in &self.instruments {
            ensure!(
                crate::observations::valid_spot_instrument(&instrument.instrument_id)
                    && instrument.instrument_id.ends_with(&instrument.inst_id),
                "instrument identity and inst_id disagree"
            );
            ensure!(
                service.contains(&instrument.inst_id),
                "observation instrument must be present in candle universe"
            );
            ensure!(
                ids.insert(&instrument.inst_id),
                "duplicate observation instrument"
            );
            canonical_ids.insert(instrument.instrument_id.as_str());
            ensure!(
                !instrument.metadata_revision.is_empty()
                    && instrument.metadata_revision.len() <= 128
                    && instrument.effective_at_ms > 0
                    && instrument.available_at_ms >= instrument.effective_at_ms,
                "invalid point-in-time instrument metadata identity or availability"
            );
            for (field, raw) in [
                ("tick size", &instrument.tick_size),
                ("lot size", &instrument.lot_size),
                ("minimum size", &instrument.minimum_size),
            ] {
                crate::observations::validate_positive_decimal(raw, field)?;
            }
        }
        ensure!(
            self.notional_bars.len() <= self.instruments.len(),
            "notional bar calibration count exceeds instrument universe"
        );
        let mut notional_ids = BTreeSet::new();
        for calibration in &self.notional_bars {
            ensure!(
                canonical_ids.contains(calibration.instrument_id.as_str()),
                "notional bar calibration instrument is outside configured universe"
            );
            ensure!(
                notional_ids.insert(&calibration.instrument_id),
                "duplicate notional bar calibration"
            );
            ensure!(
                !calibration.calibration_id.is_empty() && calibration.calibration_id.len() <= 128,
                "calibration ID is required"
            );
            ensure!(
                calibration.calibration_receipt_sha256.len() == 64
                    && calibration
                        .calibration_receipt_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit()),
                "calibration receipt SHA-256 is invalid"
            );
            ensure!(
                calibration.estimator_version == "median_completed_60s_turnover_v1",
                "unsupported notional calibration estimator"
            );
            ensure!(
                calibration.calibration_start_ms > 0
                    && calibration.calibration_end_ms > calibration.calibration_start_ms
                    && calibration.calibration_end_ms <= calibration.scored_start_ms,
                "notional calibration must end before the scored window"
            );
            ensure!(
                (1..=86_400).contains(&calibration.target_duration_seconds),
                "notional target duration must be bounded"
            );
            ensure!(
                calibration.valid_intervals >= calibration.minimum_valid_intervals
                    && calibration.minimum_valid_intervals > 0,
                "notional calibration has insufficient valid intervals"
            );
            let median = Decimal::from_str_exact(&calibration.median_valid_60s_turnover_quote)?;
            let threshold = Decimal::from_str_exact(&calibration.quote_threshold)?;
            ensure!(
                median > Decimal::ZERO && threshold > Decimal::ZERO,
                "notional threshold and calibration median must be positive"
            );
            let expected = median
                .checked_mul(Decimal::from(calibration.target_duration_seconds))
                .and_then(|value| value.checked_div(Decimal::from(60)))
                .ok_or_else(|| anyhow::anyhow!("notional threshold calibration overflow"))?;
            ensure!(
                threshold == expected,
                "notional threshold does not match the frozen calibration estimator"
            );
            let coverage = Decimal::from_str_exact(&calibration.valid_coverage_ratio)?;
            let minimum = Decimal::from_str_exact(&calibration.minimum_coverage_ratio)?;
            ensure!(
                coverage >= Decimal::ZERO
                    && coverage <= Decimal::ONE
                    && minimum > Decimal::ZERO
                    && minimum <= Decimal::ONE
                    && coverage >= minimum,
                "notional calibration coverage is below its frozen minimum"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(calibration: NotionalBarCalibration) -> ObservationSettings {
        ObservationSettings {
            schema_version: "okx.observations.config.v1".into(),
            enabled: true,
            environment: "public".into(),
            region: Region::Global,
            instruments: vec![InstrumentMetadata {
                inst_id: "BTC-USDC".into(),
                instrument_id: "okx:spot:BTC-USDC".into(),
                metadata_revision: "synthetic-meta".into(),
                tick_size: "0.1".into(),
                lot_size: "0.01".into(),
                minimum_size: "0.01".into(),
                effective_at_ms: 100,
                available_at_ms: 110,
            }],
            notional_bars: vec![calibration],
            channels: vec!["trades-all".into()],
            limits: ObservationLimits {
                max_pairs: 1,
                max_sockets: 1,
                max_subscriptions: 1,
                max_frame_bytes: 4096,
                queue_capacity: 1,
                dedup_identities: 1,
                max_replay_records: 100,
                metadata_refresh_interval_ms: 60_000,
                segment_bytes: 16 * 1024 * 1024,
                minimum_disk_free_bytes: 1,
                raw_retention_days: 1,
            },
            journal_dir: "/tmp/observations-test".into(),
        }
    }

    fn calibration() -> NotionalBarCalibration {
        NotionalBarCalibration {
            instrument_id: "okx:spot:BTC-USDC".into(),
            quote_threshold: "300".into(),
            calibration_id: "synthetic-calibration".into(),
            calibration_receipt_sha256: "a".repeat(64),
            estimator_version: "median_completed_60s_turnover_v1".into(),
            calibration_start_ms: 1000,
            calibration_end_ms: 2000,
            scored_start_ms: 2000,
            target_duration_seconds: 60,
            median_valid_60s_turnover_quote: "300".into(),
            valid_intervals: 100,
            minimum_valid_intervals: 100,
            valid_coverage_ratio: "0.95".into(),
            minimum_coverage_ratio: "0.90".into(),
        }
    }

    #[test]
    fn notional_calibration_requires_exact_scoped_causal_receipt() {
        let baseline = settings(calibration());
        assert!(baseline.validate(&["BTC-USDC".into()]).is_ok());
        let mut revised_receipt = calibration();
        revised_receipt.calibration_receipt_sha256 = "b".repeat(64);
        let revised = settings(revised_receipt);
        assert_ne!(
            baseline.observation_identity().unwrap(),
            revised.observation_identity().unwrap()
        );

        let mut future = calibration();
        future.calibration_end_ms = 2001;
        assert!(settings(future).validate(&["BTC-USDC".into()]).is_err());

        let mut guessed = calibration();
        guessed.quote_threshold = "301".into();
        assert!(settings(guessed).validate(&["BTC-USDC".into()]).is_err());

        let mut out_of_scope = calibration();
        out_of_scope.instrument_id = "okx:spot:ETH-USDC".into();
        assert!(
            settings(out_of_scope)
                .validate(&["BTC-USDC".into()])
                .is_err()
        );
    }
}
