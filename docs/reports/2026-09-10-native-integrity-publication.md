# Native integrity publication validation

## Scope and source identity

This change integrates the tested native incident, inventory, market-data health,
and Telegram admission fixes onto Hummingbot `origin/master`
`add81503f78e568d287928157c712ebbc898c8df`. The deployed preparation started from
`821753645c75713938503bdcc14eb022fbc09006`; its source files were checked against
the final immutable build manifest before integration. Existing controller
source fingerprints, restart-required execution gates, headless notifier startup,
and MQTT reconstruction checks from the newer primary branch are retained.

This is source publication preparation. It does not restart or redeploy an owner,
change strategy configuration, query an exchange, or grant trading authority.
The deployed checkout and private runtime state remain unchanged.

## Behavioral changes

- Exact fill amount, price and fee provenance survive SQLite persistence. Legacy
  six-decimal columns remain explicitly legacy; missing fee valuation stays
  unavailable. SQLite schema updates use a transaction and a consistent backup.
- Periodic held-inventory reports corroborate complete exact ledger receipts and
  preserve their replay order. Incomplete or conflicting receipts fail before
  replacing positions. Existing immutable fill identities cannot change side,
  amount, price, actual fees or recorded quote fees. New distinct exact fills
  may extend an order. Report failure retains the existing execution gate.
- Expired unfilled entries need authoritative terminal fill reconciliation before
  their outcome is accepted. Held executor result accounting retains unresolved
  outcomes instead of manufacturing a completed loss or flat result.
- Registered rate sources reject missing or stale managed quotes. The primary
  branch's per-pair refresh health, bounded cached-rate expiry and deduplicated
  recovery logging remain available. Ticker/candle observations retain source
  time separately from local observation time.
- MQTT log publishing uses a bounded worker queue so transport stalls do not
  block the strategy thread. Telegram polling requires explicit admission in
  addition to the local token lease; revocation releases polling ownership while
  notifications remain available.

## Verification

The 31 owner test modules below passed in the Python 3.13 owner environment:
**658 tests and 36 subtests**, in 27.22 seconds. The validation harness rejected
socket connections, connect-ex calls and DNS lookups. The fresh checkout reused
60 owner-built Cython extensions after checking matching `.pyx`/`.pxd` sources;
these local dependencies are not committed. Ruff on every changed Python file
and `git diff --check` passed.

The suite emits 162 existing deprecation warnings from `commlib`'s use of
Pydantic `dict()`; these are dependency warnings, not failed checks. Three
aggregation-only tests now isolate reconciliation explicitly, and the recorder
fixture remains patched for each complete test lifetime. Separate public-path
regressions exercise real reconciliation, unavailable results and gate recovery.

```bash
python -m pytest -q \
  test/test_hummingbot_quickstart.py \
  test/test_headless_native_startup.py \
  test/test_hummingbot_runtime_identity.py \
  test/test_native_lifecycle.py \
  test/test_native_sources.py \
  test/test_native_mqtt_health.py \
  test/test_okx_native_reconciliation.py \
  test/test_ok_rsi_candle_aggregation.py \
  test/test_ok_rsi_chart_snapshots.py \
  test/test_ok_rsi_held_loss_results.py \
  test/hummingbot/data_feed/test_market_data_refresh_resilience.py \
  test/hummingbot/data_feed/test_market_data_provider.py \
  test/hummingbot/data_feed/test_native_market_data_health.py \
  test/hummingbot/data_feed/candles_feed/test_candles_base.py \
  test/hummingbot/data_feed/candles_feed/okx_spot_candles/test_okx_spot_candles.py \
  test/hummingbot/model/db_migration/test_sqlite_atomic_migration.py \
  test/hummingbot/model/test_trade_fill.py \
  test/hummingbot/model/test_trade_fill_exact_receipts.py \
  test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py \
  test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py \
  test/hummingbot/strategy_v2/executors/test_held_inventory_accounting.py \
  test/hummingbot/strategy_v2/executors/test_position_reconciliation_precision.py \
  test/hummingbot/notifier/test_telegram_notifier.py \
  test/hummingbot/notifier/test_telegram_polling.py \
  test/hummingbot/notifier/test_telegram_polling_admission.py \
  test/hummingbot/remote_iface/test_mqtt.py \
  test/hummingbot/remote_iface/test_mqtt_log_isolation.py \
  test/hummingbot/strategy/test_controller_source_guard.py \
  test/hummingbot/strategy/test_strategy_v2_base.py \
  test/hummingbot/connector/exchange/okx/test_okx_exchange.py \
  test/hummingbot/client/test_performance.py
```

## Integration decisions and limits

Three-way source integration preserved primary-branch guards rather than copying
an older checkout over them. The material conflict was rate refresh: strict
registered-source invalidation and source-time telemetry coexist with the
existing refresh-health API and its bounded unregistered cache. Both sets of
behavioral tests pass. Cosmetic-only imported file changes were excluded.

Repository validation does not establish runtime adoption of the integrated
branch. A later deployment must bind the resulting source/image/configuration
identities and follow the owner stop, reconciliation and rollback procedure.
No private connector settings, passwords, databases, build contexts or runtime
receipts are included in this change.
