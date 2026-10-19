# Protected controller reload batch repair

Source gate PASS; final image adoption and live observation are separate.

A live V2 watchdog episode on 2026-10-19 at 19:42:07 UTC sampled
`StrategyV2ConfigBase._load_controller_module` resolving protected source paths
during periodic configuration reload. One hold/resume episode was present in
both Docker and native-file logs. Earlier clean observations are retained.

Five configurations use the same protected module. The old startup batch checked
its source files five times; a subsequent reload preflight checked it again
before the per-config checks. A fresh local cache now reuses a protected module
only after its complete source fingerprint passes in that same batch. A new
batch creates a new cache and revalidates. Unprotected modules keep per-config
reload behavior. Controller configuration order and multiplicity are preserved.
No signal, sizing, order, exit, source-change admission or restart policy changes.

The regression fails on previous source (five reads per source) and passes after
the change (one read). It checks the next batch, source mutation before config
reads, and legacy module reload. Injected 2 ms per source-read measurements were
0.032927 s before and 0.009482 s after; the next reload was 0.007479 s. These are
bounded test measurements. Filesystem I/O still occurs once per protected module
per batch; this repair does not promise that every possible filesystem stall is
eliminated.

Validation used the deployed V2 image
`sha256:4f29cab61b305d783ae4184230d1c6b331bfc008e8b849109e47be0d67c91c71`
with only the candidate source file and repository tests mounted read-only,
`--network none --cpus 1 --memory 1g`, and no exchange credentials:
`python -m pytest -q -p no:cacheprovider test/hummingbot/strategy/test_strategy_v2_base.py`.
Result: 74 passed, 7 subtests, one existing event-loop deprecation warning.
The earlier narrow three-test run used an in-memory test-helper shim; the final
74-test run used the actual mounted repository helper without that shim.
Ruff, compile and diff checks pass. Independent review found no source blocker.

Private evidence, exact commands and the independent review receipt are under
`~/.local/share/rsibot/releases/20261019-native-stall-adoption/reload-owner-batch-cache-evidence/`.
Previous runtime images and continuity backups remain recovery references.
Native image promotion requires qualified exact-owner builds, unchanged config
and financial history, sanctioned owner restart, API rebinding and fresh live
warning/health checks. No live order was sent as a test.
