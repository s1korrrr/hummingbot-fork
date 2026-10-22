# V2 mean-reversion context: unconnected contract/policy candidate

Canonical specification and ExecPlan: [rsibot #195](https://github.com/s1korrrr/rsibot/pull/195), `docs/plans/active/2026-09-20-v2-composite-context.md` in the root repository. This continuation adds a candle-cache mapper and a no-order native action sidecar. It is not live activation.

## Implementation HEAD

The hashes below record the earlier `eeb3c564c78f6d02a867bffac7e796d48f5e2979`
source checkpoint. The current corrected code head is
`a53df38a642896537e077b4af6a0b687aaeafa40`. Companion:
[rsibot #195](https://github.com/s1korrrr/rsibot/pull/195). Keep draft. Do not merge.

SHA-256 of published core files at that HEAD:

```
55b091121917ce9497a51e483f43a1c602990827293ea1f0ee05559b4cf08953  controllers/generic/mr_context_v2_core/__init__.py
2a575815412606d6aaf17043aa377185189af889cc4e5ef98b200093dc61733c  snapshot.py
803301e8c6f6f920589db122485426a800f59c4069ef9520e659361f1631898d  recipe.py
4c122f895749bc6fc94b1324b896435a8fd4728481748a6f5580762943c8078a  policy.py
1a00a6e6d1184f47cb9b9f031f46f5edf71d95cb4cfee2548e981cae2456029a  producer.py
20d7434b8bbd734efc5b06cc030b4b90ebbf2416b92366fd0d637aa05fecebf1  native_adapter.py
```

Owner Python `/Users/s1kor/anaconda3/envs/hummingbot/bin/python` 3.13.13, pydantic 2.13.4, pytest 9.0.3, `PYTHONPATH` = this worktree. These commands were run on that HEAD in the completed session, not rerun in the docs-receipt commit:

```sh
PYTHONPATH=$PWD /Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest -q \
  test/modular_spot/test_mr_context_v2_{snapshot,policy,isolation,native,shadow,producer}.py
# 134 passed

PYTHONPATH=$PWD /Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest -q \
  test/modular_spot/test_rsi_modular_profiles.py \
  test/modular_spot/test_recipe_compiler.py \
  test/modular_spot/test_native_composition.py \
  test/modular_spot/test_decision_protocol.py
# 64 passed
```

Overlay-only source closures contain no `mr_context` paths: `ok_rsi` 64 files digest `12f8442c5e9fc4f150cb3f474223766392919b654b7f4a632496fd97241ec8ca`; `rsi_v5` 64 files same overlay digest; `modular_spot` 37 files; `components` 24 files.

Limitations: executor orchestrator suite not rerun on `eeb3c564`; Cython `.so` built in the worktree is gitignored; hummingbot-api was not changed; every assessment has `execution_authorized=false`.

Next source step: design a reviewed pre-CAS seam for an independent opt-in host. `ModularRuntime.actions()` currently owns plan, reserve and CAS in one method; a thin subclass cannot insert the helper without copying that method or changing the source-sealed shared runtime. Neither shortcut is qualified here. T5 needs a real v2 owner program and an OKX May-August 2026 cache. T5/T6 stay HOLD.

2026-09-23 source correction: `apply_restrict_to_plan` now verifies that the
revised claim and quote commitment equal the planner's exact amount, price and
fee reservation before it can withhold or reduce a first entry. A red test
reproduced acceptance of an unbound commitment; the corrected six-file suite
passed **135 tests** on the owner Python 3.13.13 checkout. The current
`native_adapter.py` SHA-256 is
`7a426473d8b3833a4bb5e86e4067d2d45913c476e1553b3436e9735e52474e36`.
The earlier file hashes and `verification.json` remain historical receipts.
No runtime host calls this adapter yet; post-reservation action filtering is
not equivalent to native pre-CAS integration.

Later source correction: `apply_to_actions` now rejects every restrict-mode
invocation with `pre-reservation plan integration` instead of editing or
dropping already reserved BUY actions. Off and shadow remain action no-ops.
The six focused owner modules passed **130 tests** after replacing old tests
that asserted post-reservation filtering with refusal cases. The earlier
135-test count and adapter hash describe the previous checkpoint only.

## Implemented boundary

- `snapshot.py`: immutable, bounded one-minute spot/USDC context envelopes, strict JSON decoding, exact decimal units, canonical `ObservationRef`, fixed expiry, causal as-of selection, revision-conflict detection and acquisition-validity diagnostics.
- `recipe.py`: explicitly parameterized conjunctive reduction rules and a policy-envelope binding using the existing schema-1 `RecipeBinding`. Modes and thresholds are sealed. No calibrated thresholds are supplied by default.
- `policy.py`: initial-entry-only diagnostic assessment against a bound baseline proposal and supplied native capacity evidence. The most restrictive applicable multiplier wins; it is always in [0, 1]. Quote-fee-inclusive commitments and base quantities are reduced and rounded downward, with below-minimum proposals withheld.
- `admission_prerequisite`: converts restrict-mode assessments to the existing non-authorizing prerequisite, checking acquisition identity and current expiry. A future native adapter must ALSO recheck real state, source/image/config identity, quantity/fee caps and admission authority. This helper does not apply a cap to an executor.
- `producer.py`: maps `okx-candle-service` cache bytes into `ContextSnapshotV2`. Equal-weight components. Disk cache is reconstructed unless `replica_ready=True`.
- `native_adapter.py`: `apply_to_actions` rejects restrict mode because action filtering occurs after reservation; off/shadow preserve actions. `apply_restrict_to_plan` can rewrite a planned initial entry and commitment before reserve/CAS after its caller proves the original native reservation. It has no host yet. `execution_authorized` stays false. No `ControllerBase`, no `NativeComposition` import, no `runtime.py` edit.

Off mode bypasses context assessment. Reduction opportunities bypass the entire acquisition-only path. Shadow output cannot be converted to an admission prerequisite. All assessments retain `execution_authorized=false`.

## Strict limits and deliberately unsupported operations

There are no orders, reservations, balance mutations, services, sockets, database writes, configuration exports, live configuration changes or automatic startup. Live `rsi_modular_v2` does not import this package. Restrict mode is unavailable through the post-reservation action adapter; the pre-CAS plan helper is unconnected and does not enable execution. DCA, candidate ranking, exits, base/third-currency fee adapters, non-USDC execution, native paper integration and economic adapters remain unsupported.

Capacity/proposal models carry supplied read-only receipts; constructing them does not authenticate an account or establish wallet-wide spending authority. Policy source hashes are supplied by the caller and bound to the output; a genuine source/image loader is still required. Do not advertise `bind_policy` as native configuration validation or a complete source-closure attestation.

The wire decoder accepts only the v2 envelope, at most 64 KiB, and rejects Market Pulse scalar keys (`composite_score`, `momentum_score`, `feature_status`). `producer.snapshot_from_candle_cache` maps `okx-candle-service` cache JSON (`source=okx-public-spot`, USDC spot, confirmed 1m). Disk cache bytes are `reconstructed` unless the caller sets `replica_ready=True` for a live replica after `entry_ready`. Historical as-of is always reconstructed. `source_content_sha256` hashes the clipped used rows, not the whole cache file.

Returns, breadth, volatility and multipliers are fractions, monetary values use exact decimal strings/Decimal (not floats), and times are UTC milliseconds. Bar bounds are normalized to a half-open minute. Missing economic fields stay unavailable when required by a rule. Coverage uses the conservative lower bound `max(0, ready + fresh - expected)` because separate ready/fresh counts do not establish their overlap. This may withhold uncertain coverage rather than invent eligible members.

Snapshots exclude consumer receipt time from their observation hash. Rereads cannot renew event time, primary consumption identity or context expiry. Later corrections create new revisions; malformed/conflicting revisions reject. The existing decision protocol consumes a held primary event: context recovery alone is not a deferred retry mechanism.

## T0-T6 continuation

The package stays outside `hummingbot.strategy_v2.components` and `modular_spot_core`. Overlaying only `mr_context_v2_core` onto the pinned v2 execution image does not change the `rsi_modular` `source_files` closure: 64 files, digest `12f8442c5e9fc4f150cb3f474223766392919b654b7f4a632496fd97241ec8ca`, no `mr_context` paths, for both `ok_rsi` and `rsi_v5`.

T0 host: live stack-v2 `execution-rsi` runs `rsi_modular` profile `ok_rsi` (`NativeComposition` over `OKRSIController`). That is a frozen compatibility wrapper, not an independent v2 mean-reversion recipe. The compiler `NativePolicyRecipe.family` remains `ok_rsi|rsi_v5`. Connecting context by importing this package from `native_composition.py` would reseal deployed owners; this slice does not do that.

T1 producer: stack-v2 consumes `okx-candle-service` (`candles-rsi`, five USDC pairs, history 7514). hummingbot-api Market Pulse is not on that path and is not imported.

T3 adapter: the former post-reservation `apply_to_actions` filtering is retired for restrict mode. `apply_restrict_to_plan` is the pre-CAS helper for a future host, but `ModularRuntime.actions()` has no insertion point between plan and reserve/CAS. A subclass would have to duplicate the method; changing `runtime.py` would reseal `rsi_modular`. Independent review (Cursor Grok 4.6 subagent `98baf990-d392-4d58-9d4b-b6fb6e0dc9fd`) requested the pre-CAS helper. Live `rsi_modular_v2` does not load this function.

T4/T5: `UNSUPPORTED_V2_OWNER_ADAPTER`. `scripts/paper-research.sh programs list` has `okx_spot_dca`, `hyperliquid_perps`, `binance_futures_proxy`. There is no isolated v2 program. The sealed May-August corpus is `ok_rsi` / `BINANCE_SPOT_PROXY`, not OKX v2. Do not relabel `okx_spot_dca`. Keep `rsi-modular-parity-20260920-01` FAILED.

T6: HOLD. No paper/live activation, no V1/V2 restart, no overlay onto running `execution-rsi`.

## Verification actually performed

Owner source base: `0f19b464ccd112b21ac69c6de0e5515250239aac`. Root plan checkpoint: `e35b1274acc6d72c18be540aec22fb76524346a0`.

The sandbox had no complete Hummingbot checkout/runtime and could not resolve GitHub for a Git clone. Fourteen required upstream Python files were read through connected GitHub, copied byte-for-byte and verified against their Git blob hashes. Normal package imports were used, including actual upstream initializers/logger modules: no fake native classes or `sys.modules` stubs. Those unchanged dependency copies are NOT part of this PR.

Python 3.13.5, Pydantic 2.13.4 and pytest from the sandbox were used; this is not the pinned deployment interpreter/image.

```sh
PYTHONPATH=. python -m pytest -q --junitxml=../receipts/component-tests.xml
# 104 passed, no skips, 2.76 seconds; all tests available in the dependency subset.
PYTHONPATH=. python -m pytest -q --import-mode=importlib test/modular_spot
# 104 passed, no skips, 2.75 seconds; alternative import-mode verification, not additive test count.
python -m compileall -q controllers/generic/mr_context_v2_core test/modular_spot
# exit 0
```

Test-first receipts include the absent snapshot module, absent recipe module and subsequent failing regressions for coverage intersection/decimal context, bounded decoding, assessment source provenance, shadow veto preservation and cached-prerequisite expiry. Each was rerun successfully after repair. A deterministic 271-case budget sweep is one of the 104 tests, not a separate economic experiment.

Checks cover legacy-free imports, no import-time socket/thread startup, canonical model/function identity, missing/nonfinite/unknown input rejection, source/config/recipe/state binding, cold/warm/as-of behavior, prefix invariance for later available records, correction conflicts, fixed expiry, held-event consumption, minimum orders, quote-fee caps, shadow separation and down-rounding. Complete source manifests, environment and exact observed limits are in `verification.json`.

Unavailable checks were actually attempted and remain failures of the local preparation boundary, NOT product-code failures:

```text
PYTHONPATH=. python -c 'from hummingbot.strategy_v2.controllers.controller_base import ControllerBase'
ModuleNotFoundError: No module named 'hummingbot.strategy_v2.controllers'
python -m pre_commit --version
No module named pre_commit
```

The full native/legacy suites, configured repository hooks, independent audit, hosted CI, runtime observation and economics are not claimed. Re-run the focused tests and required owner suites/hooks in the complete pinned owner environment before native integration or merge readiness.

## Resumption and rollback

Next: keep T4/T5/T6 HOLD until a separately reviewed v2 research program, OKX causal corpus, and operator-authorized host opt-in exist. Do not inherit `ok_rsi`, hook `NativeComposition`, or overlay this package onto a running execution owner.

Rollback is revert of the added mapper/adapter/tests/docs. No service restart. Never reset native/research state, inventory, costs or failed attempts to clear a hold.
