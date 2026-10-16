# OKX public observations

The observation collector is an opt-in extension. The existing one-minute candle request,
recovery path, and output contract remain unchanged when `--observations-file` is absent.
The collector accepts public spot data only. It does not place orders and its status does not
qualify a source for research or trading.

## Enablement

Pass `--observations-file /path/to/settings.json` to the service. The file must be a regular
file no larger than 64 KiB, and `enabled` must be `true`. Missing configuration creates no
observation tasks. There are no production presets in this document; the following is a
synthetic shape example. Replace metadata, paths, limits, and credentials for the intended
environment and validate them before enabling capture.

```json
{
  "schema_version": "okx.observations.config.v1",
  "enabled": true,
  "environment": "public",
  "region": "global",
  "instruments": [{
    "inst_id": "BTC-USDC",
    "instrument_id": "okx:spot:BTC-USDC",
    "metadata_revision": "synthetic-metadata-revision",
    "tick_size": "0.1",
    "lot_size": "0.001",
    "minimum_size": "0.001",
    "effective_at_ms": 1792108800000,
    "available_at_ms": 1792108800100
  }],
  "notional_bars": [],
  "channels": ["trades-all", "bbo-tbt"],
  "limits": {
    "max_pairs": 1,
    "max_sockets": 2,
    "max_subscriptions": 2,
    "max_frame_bytes": 262144,
    "queue_capacity": 4096,
    "dedup_identities": 8192,
    "max_replay_records": 100000,
    "metadata_refresh_interval_ms": 60000,
    "segment_bytes": 67108864,
    "minimum_disk_free_bytes": 1073741824,
    "raw_retention_days": 7
  },
  "journal_dir": "/var/lib/okx-candle-service/observations"
}
```

`region` is `global` or `eea`; it selects an allow-listed OKX endpoint set. `trades-all`
uses the business socket. `bbo-tbt` uses the public socket and represents one displayed bid
and ask level, not a depth book. Global endpoints use the default HTTPS/TLS port 443; EEA
uses the regional `wseea.okx.com` endpoints. Each channel gets its own socket. The configured instruments
must also belong to the candle service's instrument universe. All resource limits are required
and finite. `max_replay_records` bounds the per-channel verified suffix after the latest
checkpoint. An overlong suffix fails closed and requires operator review.
`metadata_refresh_interval_ms` is required and bounded to 1-300 seconds. Segment size
must be at least 16 MiB so each bounded metadata response can be retained as one journal row.
The additive observation path uses port 443 because OKX will stop accepting WebSocket
connections on port 8443 after October 31, 2026. The existing candle collector also uses port 443 with its previous host/path, candle1m
subscription and recovery behavior. See the
[OKX port 8443 discontinuation announcement](https://www.okx.com/en-eu/help/okx-websocket-port-8443-discontinuation-announcement).
The producer emits observed source timing and bar quality. Consumers must apply their own
explicit quote freshness, feature expiry, complete-window, and indicator warmup policies.
The collector does not claim an economic consumer has passed those gates.

Quote-notional bars stay disabled with an empty `notional_bars` list until a frozen calibration
is available. A calibration entry is scoped to one canonical `instrument_id` and must carry
`calibration_id`, `calibration_receipt_sha256`, `estimator_version` set to
`median_completed_60s_turnover_v1`, calibration start/end and scored-start UTC milliseconds,
`target_duration_seconds`, `median_valid_60s_turnover_quote`, `valid_intervals`,
`minimum_valid_intervals`, `valid_coverage_ratio`, `minimum_coverage_ratio`, and
`quote_threshold`. The calibration must end no later than the scored window, meet its sample
and coverage minimums, and satisfy
`quote_threshold = median_valid_60s_turnover_quote * target_duration_seconds / 60` exactly.
Thresholds are not inferred by the collector. Bars consume whole trades and preserve threshold
overshoot. Incomplete notional bars remain open across restart via checkpoints.

## Journal, replay, and health

Raw websocket text is hashed and written as its own journal record before normalized event
records are queued. Segment I/O and fsync run on the journal worker; the socket handler waits for
each record's durable acknowledgement before mutating trade-derived state or reading the next
frame. Cross-channel work shares only the bounded queue. A full queue,
write error, manifest failure, disk-floor violation, or incomplete subscription acknowledgement
is visible as a capture failure; there is no silent drop path. Journal records have a hash chain
and segment manifest. A record is durable only after its segment bytes and manifest boundary
are synced. Streaming updates are emitted only after the durable cursor
`{segment, sequence, hash}` is acknowledged. Consumers must use the durable cursor for persisted
identity. Segment sequence numbers are local to each segment.

On a normal stop, capture finishes the in-flight frame or timer branch and its durable
dispositions before closing the journal. The task drain is bounded to 120 seconds; a timeout
is reported as an incomplete shutdown. Before a restart after that error, the owner must verify
the raw-frame dispositions from the preserved journal. Rejected frames retain the configured
socket channel as their journal lineage even when the exchange frame names another channel.
ACK and error frames have one durable disposition, including when their payload contains a
`data` array; market-data frames require a disposition for each array item.

The bounded live update also reports local `accepted_at_ms`, `durable_acknowledged_at_ms`, and
`published_at_ms`; these do not replace source `available_at_ms`. The sealed tape declares
`availability_semantics: source_receipt_only_publication_unavailable` because publication timing
receipts are not persisted in the tape. Offline consumers must hold historical decision-time or
computation-to-publication latency qualification until a persisted receipt format is added.

`raw_retention_days` is a required finite policy value, but the collector does not delete old
segments automatically. Preserve the append-only journal until an owner retention and archive
workflow can safely account for manifests, checkpoints, and sealed tape references. Instrument metadata in the opt-in configuration is a frozen expectation, not an exchange
receipt. Startup fetches the regional public SPOT catalog before opening trade or BBO sockets,
then refreshes it at the configured interval. Every response is retained before parsing. A
changed tick/lot/minimum rule, missing/non-live instrument, pending rule change, failed refresh,
or malformed response records a rejection and scoped gap, then stops observation capture while
candle service continues. Exchange rule effective time is unknown when OKX does not report it;
the normalized receipt carries `effective_at_ms: null` rather than copying the configured time.

Checkpoints bind deduplication overlap state, fixed-time buckets, notional-bar builders, and the
exact preceding durable cursor. On startup, the service verifies the journal and checkpoint,
then replays the suffix through the same trade deduplication and bar transitions before opening
new source sessions. A source reconnect records a gap and starts a new epoch. Qualified windows require
complete post-ACK coverage; duplicate replay is not assumed. Retained IDs and the
checkpointed event-time eviction floor quarantine possible replayed old prints. Corrupt durable bytes fail closed. A torn unacknowledged active tail is
preserved for diagnosis and recovery to the last manifest boundary.

When observation capture is configured, authenticated read-only endpoints are available:

- `GET /v2/observations/health`
- `GET /v2/observations/capabilities`
- `GET /v2/observations/stream?pair=BTC-USDC`

Use the service's existing bearer authentication. Pair scopes are enforced on streaming
connections; a lagged reader must reset from a new health snapshot. Capabilities identify the
source as not independently qualified and state that stream resume requires reset. The stream
provides live updates only. Sealed journal files are the historical replay interface.

## Failure isolation and rollback

Observation configuration, journal, or capture failures cancel only the observation task.
They must not stop the existing candle collector. Check `/health` and the observation health
endpoint to confirm both paths after an error. Preserve the journal directory and its manifest
for diagnosis; do not edit or truncate durable files by hand.

To disable collection, remove `--observations-file` from the service invocation and restart
through the normal owner process. This starts no observation sockets and leaves the candle
request path in its existing mode. Keep the prior journal directory for recovery and audit.
Re-enabling requires the same compatible instrument universe, limits, channel set, and frozen
notional calibration identities as the saved checkpoints, or an explicit reviewed migration.

## Local validation

Run from this service directory:

```sh
cargo fmt --all -- --check
cargo test --locked --offline
cargo clippy --locked --offline --all-targets -- -D warnings
```

The local websocket tests use loopback-only synthetic frames. They do not connect to OKX,
validate live credentials, prove full market coverage, or authorize capture in a deployed
service.

## Native completed-bar projection

`hummingbot.strategy_v2.market_context.observation_bars.enrich_completed_bar` enriches a
completed Rust time/notional bar using complete ordered normalized members supplied by a
verified journal traversal. It reuses native `flow_window` for side quote totals and exposes
side base/counts, VWAP and actual-duration rate. A notional bar of zero duration has no rate.
The projection validates exact 60-second time alignment, totals/OHLC, explicit frozen notional
threshold/overshoot and the notional rolling membership digest. The required external
membership receipt binds the bar, source identity and all normalized economics. Its producer
and interval coverage must be independently verified: a caller-generated hash alone cannot
prove journal completeness or an empty interval. The bounded diagnostic ID sample is
insufficient to supply complete membership. No event indicator or faster decision policy is
activated by this helper.
