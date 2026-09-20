# OKX candle service

A standalone, read-only public market-data producer for confirmed OKX spot USDC
one-minute candles. Native Hummingbot executors and accounting remain consumers'
responsibility. This service has no exchange credential, order, administrative,
or deployment endpoint. It does not grant trading authority.

## Build and run

Requires Rust 1.88 or newer on Unix (verified with Rust 1.97.1), DNS and outbound
TLS access to the fixed public OKX endpoints. Cargo.lock pins dependencies.

```sh
cargo build --release --locked
./target/release/okx-candle-service \
  --universe /absolute/path/universe.json \
  --token-file /absolute/path/local-stream-token \
  --listen 127.0.0.1:8766 --history 90
```

The universe is exactly `{"pairs":["BTC-USDC","ETH-USDC"]}`: unique uppercase
spot USDC names, 1–2000 entries. The supplied catalog should be captured from
OKX's live spot-instrument list by the operator; the service never infers pairs.
History is 2–10,000 candles, default 90; the universe times history must not
exceed 3,000,000 configured rows. The token file must be a non-symlink regular
file with permissions 0600 and contain a randomly generated 32–128 character
URL-safe token (letters, digits, `_`, `-`, optional trailing newline). Token
contents are never logged. Bind addresses other than 127.0.0.1 are rejected.
Both endpoints require `Authorization: Bearer TOKEN`; browser Origin headers
are refused for the stream. SIGINT/SIGTERM stop only this process and its sockets.

## Wire protocol v1

Connect WebSocket `/v1/stream` and within five seconds send:

```json
{"v":1,"op":"subscribe","pairs":["BTC-USDC"],"history":90}
```

Pairs must be a unique nonempty subset of the catalog and requested history must
be within configured capacity. Every server JSON frame includes `v:1`, `type`,
a process-unique `epoch`, and floating-point `published_at` Unix seconds.
Per-pair sequence starts at zero and increases for every emitted state change.

- `snapshot`: `pair`, `sequence`, `received_at`, `rows` ascending confirmed
  candles. Initial/recovery state; never an entry event.
- `candle`: `pair`, `sequence`, `received_at`, `row`. Contiguous append or latest
  bar correction. Identical duplicates and older bars are ignored.
- `status`: `pair`, `sequence`, `ready:false`, `reason`. Invalidates that pair;
  recovery always emits a fresh snapshot before further deltas.
- `heartbeat` every two seconds: `states` mapping every subscribed pair to
  `{ready,sequence}`. Heartbeats never refresh candle age. Sequence is serialized
  behind all earlier events; a client must reject epoch changes or gaps and
  reconnect/reseed.

A row has numeric integer `open_time` (Unix seconds), decimal **strings** `open`,
`high`, `low`, `close`, `volume` (base units), `quote_volume` (USDC), and
`confirmed:true`. Parsing uses exact decimal validation, minute alignment,
confirmation/closure, positive prices, nonnegative volumes and OHLC geometry.
Unsupported precision, scientific notation upstream, malformed values and future
rows fail closed. Decimal strings are normalized without conversion to floats.

`received_at` records upstream receipt wall time (the latest contributing receipt
for a snapshot). It measures local distribution delay, not venue confirmation
latency. For latest-bar corrections, compare rows by timestamp and values.

`GET /health` returns `{v,epoch,published_at,history,max_history,ready,counters,pairs}`;
`max_history` is 10000 and `history` is the configured capacity.
`ready` is true only when every catalog pair is ready. Each `pairs[PAIR]` has
`ready`, `online` (subscription acknowledged on a live socket), `sequence`,
`rows`, `reason`, `received_at`, and nullable `last_open_time`. Counters are
monotonic within an epoch and absent keys mean zero; they include
`connection_attempts`, `disconnects`, `rest_requests`, `seed_failures`,
`snapshots`, `confirmed_rows`, `candle_events`, `duplicates_ignored`,
`old_rows_ignored`, `invalidations`, `upstream_frames`, `buffered_before_ack`, `text_pings`,
`text_pongs`, `parser_failures`, `client_disconnect_errors`, `cache_loaded`,
`cache_missing`, `cache_rejected`, `cache_writes`, `cache_write_failures`, and
`warm_restores`.

## Bounded collection and recovery

The collector uses fixed `wss://ws.okx.com:8443/ws/v5/business` (`candle1m`) and
`https://www.okx.com/api/v5/market/candles` for the latest page, followed by
`https://www.okx.com/api/v5/market/history-candles` for older pages. Both REST
paths fix `bar=1m`; each page has at most 300 rows and older pages use
`after=oldest_returned_open_time_ms`. The first page includes allowance for a
forming bar. Pagination must move strictly backward and uses at most
`ceil(history/300)+2` requests per attempt. All connections use verified TLS; REST environmental proxies and redirects are disabled and the
WebSocket connector connects directly without redirect following. No endpoint
can be overridden. See [OKX API documentation](https://www.okx.com/docs-v5/en/).

Each upstream socket owns at most 100 pairs. Attempts are paced at least 1050ms
apart across this process, including retries, with 1–30 second reconnect backoff.
Text ping runs every 20 seconds even under continuous data traffic; a missing
text pong after 10 seconds disconnects the group. Every pair requires an explicit
subscription acknowledgement. Requested-pair data arriving before its
acknowledgement is validated and buffered within the same history bound, but
cannot enable readiness or REST seeding until that acknowledgement arrives;
the acknowledgement deadline remains enforced. These are process-wide limits; other applications
sharing the public IP require their own headroom.

REST seeds/backfills use at most four concurrent requests and one request per
210ms across the process, with bounded bodies and deadlines. Failed seeds remain
unavailable and retry no more often than every ten seconds. Current-generation
WebSocket rows override overlapping REST cache rows. Recovery requires exactly
configured history length, contiguous one-minute timestamps, and a latest close
no older than 90 seconds after merging current-generation WebSocket rows. Large
cold histories may take many minutes across a large universe under the public
request budget. The service stays unavailable until the complete history exists;
it never shortens the requested warmup. Disconnects immediately invalidate the socket's pairs;
in-flight seeds from its old generation are discarded. Malformed candle rows for
a known pair invalidate only that pair and its in-flight seed; structural socket
protocol failures invalidate the socket group. Gaps and stale confirmed
history invalidate pairs and request a new full snapshot. New listings with
insufficient history remain unavailable.

Each pair stores at most configured live-history rows plus one bounded recovery
baseline while unavailable. Histories and immutable snapshot events share candle
allocations through reference counting. The producer broadcast backlog is 4096
events; snapshots store shared row references and become JSON only as they are
sent. They never absorb later corrections or deltas. Initial subscribe registers
the receiver before sampling state and materializes one pair's snapshot at a
time; queued events already covered by snapshots are discarded. Slow/lagging clients
are disconnected and must reseed, never silently lose confirmed events. Client
writes have a two-second deadline; at most 32 authenticated streams are allowed.
Upstream frames are limited to 1MiB, REST bodies to 256KiB, local input to 128KiB.

## Persistent warm restart

Add `--cache-dir /absolute/path/private-cache` to opt in. Without it there are no
cache reads or writes. The directory must be a non-symlink directory with mode
0700; a missing directory is created. It contains one mode0600 JSON file per
pair with schema version, fixed OKX spot/1m identity, capacity, save time, exact
confirmed rows and SHA256 over canonical rows. Writes use a unique temporary
file, file fsync, atomic rename and directory fsync. Shutdown first joins the
collector, then performs a final cache flush from its retained complete, fresh
history before joining the cache writer. This does not restore offline readiness.
At most one pair is copied
for a disk write at a time; changed ready histories flush every five seconds.
This is a public-data cache, never a trading/account database or durable event log.

Loading checks file type/mode/size, identity, checksum, timestamp bounds, exact
decimal and OHLC validation, full declared length and one-minute continuity.
Corrupt, partial, insufficient, future-dated or older-than-24-hour records are
reported and ignored. A shorter configured request can use the tail of a larger
valid record. Changing to a longer request requires a complete cold seed.

**A loaded cache never enables readiness.** Fast restore requires acknowledgement
on the current live socket and validated current-generation market data that
matches the history's confirmed tail: either a confirmed tail/contiguous update
for the latest closed minute, or a forming head exactly one minute after that
tail. The head must match the current UTC minute at receipt and again when
readiness is evaluated; replayed prior-minute heads cannot warm a cache. Forming data proves the
live minute only and never enters confirmed rows. The merged history must still
be complete, contiguous and fresh. Restore emits a new-epoch recovery snapshot,
never historical candle deltas or entry eligibility. Reconnection within the same
process similarly requires a new acknowledgement and new-generation data.

A stale or gapped baseline remains unavailable while REST fetches the missing
suffix, including overlap with its tail. REST supersedes disk/previous-generation
rows; current-generation WebSocket rows supersede REST. A baseline older than the
requested window is discarded in favor of a bounded cold seed. In-flight results
from an older connection generation or a completed warm restore cannot overwrite
current state. This allows fresh cached universes to recover without one REST
request per pair while retaining fail-closed behavior when current proof is absent.

Warmup must be selected for the actual recipe: native RSI V5 requests at least
500 bars per configured interval; native OK RSI episode context retains 3660
one-minute bars. Deriving 500 complete 15-minute bars from one-minute history
requires at least 7500 bars plus alignment allowance. The service remains a 1m
producer; higher-timeframe aggregation belongs to the native adapter/controller.

## Linux image build

`Dockerfile` builds the locked release binary in a Rust 1.88 Debian builder and
runs it as UID/GID 10001 in a CA-enabled Debian runtime. Build from this directory
with `docker build -t okx-candle-service:local .`. The image has no embedded token,
universe or cache. Mount those explicitly with permissions readable by the chosen
runtime UID. The listener remains 127.0.0.1: a Linux container consumer must share
its network namespace (for example an explicitly chosen host network), not rely
on publishing a wildcard port. Building this image does not deploy it; no live
service is restarted by this repository change. Linux image execution requires
separate recorded validation; native Cargo checks alone do not prove it.

## Validation and operating boundary

```sh
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
```

Tests cover the shared native parser fixture, exact decimal and malformed input
rejection, bounded/contiguous history, duplicates/corrections, gap recovery,
WebSocket-over-REST merge precedence, old-generation seed rejection, per-pair
parser-failure isolation, explicit bounded-broadcast lag reporting, local auth,
snapshot/delta/status/heartbeat ordering and graceful local shutdown. Offline
WebSocket peers also verify that busy data cannot postpone a missing-pong
deadline, valid text pongs sustain the session, missing acknowledgements time
out, and unexpected EOF followed by a new acknowledgement still requires reseeding.
Pre-acknowledgement requested data stays unavailable until acknowledgement and
seed, while unrequested-pair data is rejected.
Production endpoint and timing configuration cannot be changed through the CLI;
short session timings are private to the offline tests. The shared
fixture lives at `../../test/modular_spot/fixtures/rust_candle_parity.json`.

Public integration latency and the 900-second universe/restart smoke are recorded
by the parent Hummingbot harness, separately from these unit/local transport
checks. Starting this service against public data never changes live stack
configuration. Rollback is to stop its named process and keep consumers on their
previous default transport; production deployment is a separate action.


## Hummingbot integration

The native modular data adapter is opt-in. In a native configuration, set
`data.mode: shared`, `data.transport: stream`, the authenticated origin and token
file, and `max_records: 90`. A sealed recipe instead selects
`data_transport: stream` and `history: 90` before export; do not edit a sealed
configuration afterward. The disabled reference is
[`rust-stream.json`](../../docs/controllers/modular_spot/recipe-examples/rust-stream.json).
It uses `account: unassigned` and grants no trading authority. Recipe export
continues to require the genuine native Hummingbot runtime. Existing recipes and
native configurations retain snapshot transport by default.

The collector admits confirmed OKX USDC spot 1m histories up to 10000 candles.
The matching native adapter must explicitly admit the requested history and frame
size. A longer warmup requirement must never be silently shortened. The example's
90 bars remain a small diagnostic configuration, not a universal strategy warmup.
The live OK RSI/RSI V5 owners have not been migrated or enrolled.

The tested topology is one Unix host with the Rust service and two Python data
consumers on authenticated loopback. For containers, loopback addresses belong
to their network namespace. A production layout must deliberately colocate the
reader and service network namespace or provide a separately validated TLS proxy;
this example is not a drop-in Docker deployment. Do not expose the bearer stream
by changing the bind address or disabling TLS validation in consumers.

The 15-minute public evidence, source seals and production-readiness limits are
recorded in `docs/controllers/modular_spot/RUST_CANDLE_VALIDATION.md` in this
Hummingbot checkout. The test exercises actual native DataPool reads, not native
order placement or a deployed Hummingbot API service.
