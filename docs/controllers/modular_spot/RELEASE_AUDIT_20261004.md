# V2 runtime root causes — 4 October 2026

Live V2 (`rsi_modular`) ran at 86–103% of a core and logged an hourly
`Price refresh incomplete (TimeoutError)`. A 60 s py-spy profile of the running
owner (sidecar attached with `--pid=container:`, owner unchanged) showed the
main event-loop thread at ~59% of a core against ~10% for V1 on the same
engine. Each cause below was fixed and the owner re-profiled.

| Cause | Change |
| --- | --- |
| Every 10 s config reload re-derived the controller import closure with `ast.parse` for every controller (~2.6 s event-loop stall per reload) | `native_composition.import_references` caches import references by source-content hash; the closure walk and its result are unchanged (tested against the uncached walk) |
| `CandleSnapshot.frame` rebuilt a 7,514-row frame from Decimal rows several times per tick | the float frame of a frozen snapshot is converted once, keyed by identity and evicted with the snapshot; age, warm-up and finite checks still run on every call and callers still receive copies |
| `DataPool.snapshot` built a new, revalidated snapshot per call and checked freshness by discarding two full frame copies | the cut is reused while the replica's frozen source object is unchanged; freshness uses copy-free `check()` methods applying exactly the `frame()` admission checks |
| commlib 0.11.5 RPC endpoint threads polled their stop event every 1 ms | `hummingbot/remote_iface/commlib_compat.py` blocks on the same event (engine-wide, outside this closure) |

Startup signals: the first controller action pass precedes the orchestrator's
deferred restore, so the unchanged fail-closed reconciliation hold now logs at
INFO for 60 s after the first pass (WARNING afterwards). A shared-candle
readiness projection (`custom_info.candle_readiness`) is exported; it never
changes a gate.

Measured on the live owner after deployment: main thread 59% → 19% of a core,
commlib threads 23% → 0%, container ~22%; no ticker timeout or other warning
in the first 30 minutes of log monitoring (the fault previously recurred hourly).

## Source identity

The V2 modular closure deliberately changes from
`05dd2199882fcf1662dde6b774e7613e437c292586ffdf34fe0b28f798db011a` to
`f8fff1774cb401f06097bf55aee534d066f142db68eb209d13a88d8612d2308a`.
Both manifests contain the same 67 paths; only
`modular_spot_core/native_composition.py` and the strategy components
`candle_data.py`, `candle_stream.py` and `data_reader.py` change. No path is
added or removed. Historical research receipts, native checkpoints and deployed
bindings keep their actual identities; the live V2 controllers carry no
`recipe_binding`. The deployed V2 owner carried its funding ledger and held
recovery across this code upgrade through the existing native mechanism
(logged), with a verified continuity restart.
