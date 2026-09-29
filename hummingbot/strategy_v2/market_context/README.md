# Shared market context (neutral package)

Observation-only contracts and kernels for the shared market-context design
(rsibot PR #296, `docs/plans/active/2026-09-27-shared-market-context/`).
Nothing here grants order, target, capital, risk-limit or inventory authority.
Modules import no controller family, perform no I/O at import and read no
implicit clock. `provenance.package_source_digest()` reads this package's own
files only when an owner calls it.

| Module | Task | Content |
|---|---|---|
| `contracts.py` | T1 | Snapshot, feature, requirement, selection, notification and status contracts; strict parsing; `validate_context` |
| `universe.py` | T2 | `select_reference`, capped weights, consumer entry/management scopes |
| `alignment.py` | T3 | Candle observation/block/batch contracts, `align_panel`, per-feature coverage gates |
| `features.py` | T4 | F01-F04 batch kernels, `FeatureConfig`, named `RESEARCH_V1` |
| `state.py` | T4 | Builder identity and monotonic cutoff cursor |

## Placement

The spec proposed `strategy_v2/components/market_context/`. This package is a
sibling instead. `components.source_identity.component_source_files()` seals
every file under `components`, so a new file there would change the source
digest of every existing recipe (ok_rsi, rsi_v5, rsi_modular, modular_spot,
Meridian). `test_market_context_seal_isolation.py` checks that the package
stays outside those seals.

## T0 inventory (2026-09-27; source inspection, not deployed-image evidence)

- Pins: root `origin/main` 76303f63 records gitlinks hummingbot 01842337 and
  hummingbot-api b291bda. This work starts from hummingbot `origin/master`
  1547a623e and API `origin/main` ce6466a. The API image installs Hummingbot
  from `.github/hummingbot-source.lock`, which is currently 771a5fa and does
  not contain this package.
- Candle ingress: Hummingbot `services/okx-candle-service` (Rust) runs as the
  stack-v2 `candles-paper` (history 90) and `candles-rsi` (history 7514)
  services. Each is a localhost, token-authenticated WS wire-v1 service that
  publishes confirmed OKX spot USDC 1m rows with `epoch`, per-pair `sequence`,
  `published_at` and `received_at`. Latest-bar corrections are separate
  messages. Its maximum history is 10000.
- Consumers read that service through
  `strategy_v2/components/candle_stream.CandleStreamClient` (a `Replica`).
  The API adapter subclasses that client, so it opens no exchange socket.
- History gap: the `research_v1` estimator needs 10141 closes for the 60m
  levels and 10146 closes for the 5m changes. Seeding from `candles-rsi`
  (7514 closes) therefore starts `WARMING`. The adapter keeps up to 10146
  closes in memory, so warmup takes about 44 hours of uninterrupted uptime.
  A worker restart discards that buffer.
- Storage for T5: stack-v2 renders no Postgres. The store is a dedicated SQLite
  file (stdlib) using `BEGIN IMMEDIATE` write transactions and
  `synchronous=FULL`. Lease generation and epoch are rechecked inside every
  write transaction. A payload is persisted in one transaction, the latest
  pointer advances in a second, and notification happens after both.
- API anchors: routes are registered in `main.py` behind the existing HTTP
  Basic `auth_user`. The API has no finer read scope. `MqttEventBus` and
  `InMemoryEventBus` satisfy the worker's notification port. The worker's
  command-line entrypoint currently runs with notification set to `none`
  (consumers poll).

## Consumers and cold start (2026-09-29)

- Meridian v3 binds this package **observe-only** (`market_context.mode: off | observe`,
  `controllers/generic/meridian_v3_unified_core/market_context.py`). It records the
  snapshot identity or an UNAVAILABLE/WARMING/STALE state per decision cycle and is never
  read by a decision; no gating mode exists (config validation refuses one).
- The API worker now checkpoints retained input revisions in its fenced store and restores
  them on restart, and can hydrate missing bars once at startup from OKX public history
  within explicit page/time bounds (`services/screener_core/market_context/history.py`).
  Hydrated bars are available no earlier than their page receipt; readiness stays WARMING
  until the window is complete.
- The API lock is still 771a5fa; see the API README for the requirements check and the
  placeholder candidate commit.
