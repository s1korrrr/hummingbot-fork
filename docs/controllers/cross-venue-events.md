# Cross-venue event observer

Status: research-only candidate; no execution path. This is not a tradable release.

`controllers/generic/cross_venue_event.py` subclasses the native `ControllerBase`,
not OK RSI or HL RSI. All additions are isolated. Existing controllers, shared
executors, order handling, account inventory and configuration remain unchanged.

## Contract

The pure `cross_venue_event_core.py` owns event/market input validation and
research decisions. Quants Lab imports that exact module; it does not maintain a
second policy implementation. All timestamps are UTC milliseconds in 2000-2100;
seconds, microseconds, booleans and non-finite prices fail validation.

Information is available no earlier than the maximum of receipt, normalized-event
parsing, classification and canonical-identity availability. Publication time
never substitutes for receipt. The pre-event reference must already have been
known when the event was received. Revisions cannot rearm an announcement.
Negative event latches do not expire and cannot be silently cleared by a later
positive revision. They only affect observations here, not other controllers.

Positive eligibility requires a novel external spot-listing announcement, a
pre-existing OKX continuous spot market, EEA/account eligibility evidence, allowed
quote currency, fresh non-crossed quotes, available features and bounded spread
and price extension. Public OKX metadata is not account permission. Canonical
asset identity is supplied by reviewed evidence, not inferred from ticker text.

The default 60-second delay, 20-minute lifetime, 50-basis-point spread limit,
300-basis-point chase limit and optional reaction thresholds are uncalibrated
research hypotheses, not profitable or promoted settings.

## Snapshot interface

Read one JSON object with exactly `schema`, `generated_ms`,
`risk_history_complete`, `events`, `instruments` and `quotes`.
Schema is `cross_venue_event.snapshot.v1`. Dataclass fields define row contracts.
The Quants Lab CLI produces the snapshot from its append-only archive and an
explicit market-evidence JSON file. A missing quote or account evidence yields
an unavailable decision, not an assumed value.

`risk_history_complete` refers only to preserving all known local risk labels in
the projection. It does not claim complete global news coverage. The archive's
coverage status remains UNVERIFIED. Files must be bounded regular files, not
symlinks/FIFOs. Stale/future/partial/invalid input produces UNAVAILABLE.

## No-order invariants

Configuration accepts only `execution_enabled=false`, zero total quote allocation,
and no initial inventory. `determine_executor_actions()` is always empty;
`validate_executor_config()` always denies; `send_actions()` rejects nonempty
lists. These are intentional hard boundaries, not an enable flag to turn on later.
No script, controller YAML, default service or stack deployment is added.

A later trading implementation needs an approved event-exit/loss policy, native
PositionExecutor semantics, allocation/reservation and ownership admission,
reconciliation, recovery, limits, real data, independent audit and operator
activation. In particular, native market/time-limit exits must not override the
existing profit-only inventory mandate accidentally. Do not adopt SUI/RSI holdings.

## Verification

From the Hummingbot owner source:

```sh
python -m unittest discover -s test/controllers/generic -p 'test_cross_venue_event*.py' -v
python -m compileall -q controllers/generic/cross_venue_event.py controllers/generic/cross_venue_event_core.py
```

Creation-environment result: 34 passed; 2 native dependency tests skipped.
Native loader/runtime validation remains NOT RUN, not a green integration test.
The tests intentionally do not replace the native engine with stubs. The staged
source was tested with Python 3.13.5, not the deployed engine image.
