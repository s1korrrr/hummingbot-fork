use anyhow::{Context, Result, bail, ensure};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_HISTORY: usize = 10_000;

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs_f64()
}

pub fn valid_pair(pair: &str) -> bool {
    pair.strip_suffix("-USDC").is_some_and(|base| {
        !base.is_empty()
            && base.len() <= 24
            && base
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Candle {
    pub open_time: u64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: String,
    pub confirmed: bool,
}

fn decimal(value: &Value, positive: bool) -> Result<(String, Decimal)> {
    let s = value.as_str().context("decimal must be string")?;
    ensure!(
        !s.is_empty() && s.len() <= 64 && s.bytes().all(|b| b.is_ascii_digit() || b == b'.'),
        "invalid decimal syntax"
    );
    let d = Decimal::from_str_exact(s).context("decimal outside exact supported precision")?;
    ensure!(
        d >= Decimal::ZERO && (!positive || d > Decimal::ZERO),
        "invalid decimal sign or zero price"
    );
    Ok((d.normalize().to_string(), d))
}

/// OKX spot row: ts,o,h,l,c,baseVol,volCcy,quoteVol,confirm.
///
/// Strict zero-skew contract used for cache revalidation and the shared parity fixture.
/// Live ingestion uses [`parse_row_tolerant`] instead.
pub fn parse_row(value: &Value, received_at: f64) -> Result<Option<Candle>> {
    let a = value.as_array().context("candle must be array")?;
    let (confirmed, open_time) = row_fields(a)?;
    ensure!(open_time as f64 <= received_at + 5.0, "future candle");
    if confirmed {
        ensure!(
            open_time as f64 + 60.0 <= received_at,
            "confirmed candle before close"
        );
    }
    let candle = build_candle(a, open_time)?;
    Ok(confirmed.then_some(candle))
}

/// Published status compatibility field describing the historical confirmed-bar clock skew.
/// Confirmed bars are now withheld until their close; this value is not an early-acceptance
/// allowance. Keep it stable for existing metrics consumers.
pub const CLOCK_SKEW_SECS: f64 = 2.0;
/// A forming (unconfirmed) bar may lead the local clock by at most this much.
pub const FORMING_LEAD_SECS: f64 = 5.0;

/// Outcome of the production row parser. Timing problems are classified, not errors, so one
/// early/forming bar can never invalidate a pair or fail a whole REST page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Parsed {
    /// Confirmed, closed at or before local receipt time and fully validated.
    Confirmed(Candle),
    /// Confirmed and valid, but its close is still ahead of local receipt time. Hold it and
    /// re-evaluate only when the clock reaches `open_time + 60`.
    Deferred(Candle),
    /// Valid forming bar at or before the local minute: liveness evidence only.
    Forming(u64),
    /// Unconfirmed bar that is malformed or too far ahead: ignored, never history.
    Ignored(&'static str),
}

fn row_fields(a: &[Value]) -> Result<(bool, u64)> {
    ensure!(a.len() == 9, "candle must have nine fields");
    let confirmed = match a[8].as_str() {
        Some("0") => false,
        Some("1") => true,
        _ => bail!("invalid confirmation flag"),
    };
    let ts = a[0].as_str().context("timestamp must be string")?;
    ensure!(
        !ts.is_empty() && ts.bytes().all(|b| b.is_ascii_digit()),
        "invalid timestamp syntax"
    );
    let ts: u64 = ts.parse()?;
    ensure!(
        ts > 0 && ts.is_multiple_of(60_000),
        "timestamp must align to minute milliseconds"
    );
    Ok((confirmed, ts / 1000))
}

fn build_candle(a: &[Value], open_time: u64) -> Result<Candle> {
    let (open, o) = decimal(&a[1], true)?;
    let (high, h) = decimal(&a[2], true)?;
    let (low, l) = decimal(&a[3], true)?;
    let (close, c) = decimal(&a[4], true)?;
    let (volume, _) = decimal(&a[5], false)?;
    decimal(&a[6], false)?;
    let (quote_volume, _) = decimal(&a[7], false)?;
    ensure!(
        h >= o && h >= c && l <= o && l <= c && l <= h,
        "inconsistent OHLC"
    );
    Ok(Candle {
        open_time,
        open,
        high,
        low,
        close,
        volume,
        quote_volume,
        confirmed: true,
    })
}

/// Production parser for upstream OKX rows. Structure, decimal and geometry violations of a
/// *confirmed* row stay hard errors. An unconfirmed (forming) row never errors: it is liveness
/// evidence when plausible and is otherwise ignored. Every confirmed row received before its
/// local close is `Deferred`; it becomes `Confirmed` only at or after close.
pub fn parse_row_tolerant(value: &Value, received_at: f64) -> Result<Parsed> {
    let a = value.as_array().context("candle must be array")?;
    let (confirmed, open_time) = match row_fields(a) {
        Ok(fields) => fields,
        Err(error) => {
            // Only a readable `confirm == "0"` row may be ignored; every other defect is fatal.
            if a.len() == 9 && a[8].as_str() == Some("0") {
                return Ok(Parsed::Ignored("forming_malformed"));
            }
            return Err(error);
        }
    };
    if !confirmed {
        if open_time as f64 > received_at + FORMING_LEAD_SECS {
            return Ok(Parsed::Ignored("forming_future"));
        }
        return Ok(match build_candle(a, open_time) {
            Ok(_) => Parsed::Forming(open_time),
            Err(_) => Parsed::Ignored("forming_malformed"),
        });
    }
    let candle = build_candle(a, open_time)?;
    if (open_time + 60) as f64 <= received_at {
        Ok(Parsed::Confirmed(candle))
    } else {
        Ok(Parsed::Deferred(candle))
    }
}

pub fn history_valid(rows: &[Candle], capacity: usize, at: f64) -> bool {
    history_refs_valid(rows.iter(), capacity, at)
}
pub fn history_refs_valid<'a>(
    rows: impl IntoIterator<Item = &'a Candle>,
    capacity: usize,
    at: f64,
) -> bool {
    let mut previous = None;
    let mut count = 0;
    for row in rows {
        if previous.is_some_and(|ts| row.open_time != ts + 60) {
            return false;
        }
        previous = Some(row.open_time);
        count += 1;
    }
    count == capacity
        && previous.is_some_and(|ts| at - (ts + 60) as f64 <= 90.0 && (ts + 60) as f64 <= at)
}

/// Revalidate a cached wire candle through the same exact upstream parser.
pub fn validate_candle(row: &Candle, at: f64) -> Result<()> {
    ensure!(row.confirmed, "unconfirmed cache candle");
    let timestamp = row
        .open_time
        .checked_mul(1000)
        .context("cache timestamp overflow")?;
    let raw = serde_json::json!([
        timestamp.to_string(),
        row.open,
        row.high,
        row.low,
        row.close,
        row.volume,
        row.volume,
        row.quote_volume,
        "1"
    ]);
    let parsed = parse_row(&raw, at)?.context("unconfirmed cache candle")?;
    ensure!(parsed == *row, "cache decimals must be canonical");
    Ok(())
}
