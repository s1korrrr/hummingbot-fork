# Hyperliquid Perpetual Episode Parity V2 Repair

**Date:** 2026-08-25
**Branch:** `feat/andrzej_hyperliquid_rl_episode_parity_v3`
**Status:** local read-only repair candidate; independent exact-head parity review pending
**Promotion verdict:** not evaluated; this artifact grants no paper, canary, live, capital, Risk, execution, order, or trading authority

## Observed defect

The legacy v1 parity input accepted a caller-supplied SHA-256-shaped string but never read the quants-lab artifact it claimed to identify. Its comparator saw one terminal account snapshot and five liquidation-NAV fields only. Therefore identical terminal NAV with different funding/fee/spread/slippage/impact/borrow/liquidation/external-cashflow attribution could report `MATCH`, and zero or negative terminal NAV raised before evidence could be published.

V1 parsing and verification remain available for historical authority-false evidence. V1 is explicitly insufficient for new parity-readiness claims.

## Scoped path map

- Controller: no controller signal, entry, exit, sizing, or risk behavior changes. The pure accounting boundary remains under `controllers/directional_trading/rsi_v5_core/`.
- Executor: no executor construction, lifecycle, fill, or order-state mutation. V2 consumes already reconciled fill IDs and timestamps as identity evidence.
- Connector: no Hyperliquid connector calls or exchange I/O. V2 consumes snapshots and artifact files only.
- Config/wire: `hummingbot.perp_episode_parity_run.v2` supplies Hummingbot before/after snapshots, full attribution, terminal settlement mode, fill identity, tolerance, and the expected raw quants artifact SHA-256. `quants_lab.perp_episode_parity_artifact.v2` supplies complete expected episode economics and all-false authority.
- Runtime: `scripts/run_perp_episode_parity_v2.py` reads the two regular non-symlink inputs and atomically publishes a result. Both `scripts/verify_perp_episode_parity_v2.py` and `scripts/verify_perp_episode_parity_pair_v2.py` require and re-evaluate the exact input/artifact/result triple before emitting a match label.

## V2 comparison surface

V2 compares:

- episode identity;
- liquidatable NAV before and after;
- performance NAV after external cashflow;
- tagged reward (`LOG_RETURN`, `LIQUIDATED`, or `INSOLVENT`);
- gross market PnL;
- funding cashflow;
- fee, spread, slippage, impact, borrow, and liquidation-penalty costs;
- close-cost reserve change;
- ending external cashflow;
- terminal settlement mode;
- terminal outcome;
- exact ordered fill IDs, one-to-one fill timestamps, and first/last fill timestamps;
- exact before/after snapshot timestamps.

Numeric comparisons use the declared deterministic tolerance. Identity and enum comparisons are exact. All mismatch fields are recomputed by persisted-result verification; editing status or mismatch arrays cannot forge a `MATCH`.

## Artifact-byte binding

The runner reads the quants artifact as raw bytes from a regular non-symlink file and computes SHA-256 over those exact bytes. It rejects the run before semantic comparison when that observed hash differs from the run declaration. Rehashing a semantically changed artifact cannot preserve `MATCH`: the complete parsed episode is still compared field by field. Pair verification rereads and rehashes all three files.

## Terminal failure evidence

- positive terminal NAV -> settlement-specific `FLAT`, `CARRIED`, or `ACTIVE` with finite `LOG_RETURN`;
- zero terminal NAV -> `LIQUIDATED` with explicit terminal reward `{kind: LIQUIDATED, value: null}`;
- negative terminal NAV -> `INSOLVENT` with explicit terminal reward `{kind: INSOLVENT, value: null}`.

The legacy `liquidatable_nav()` function keeps its positive-NAV requirement for v1 callers. V2 uses `terminal_nav_evidence()` so liquidation and insolvency become persisted evidence rather than exceptions or missing rows.

## Assurance repair

Independent parity review found that the first standalone v2 verifier checked a persisted result and artifact hash without re-evaluating Hummingbot input. Coordinated semantic edits plus recomputed artifact/result hashes could therefore receive a misleading standalone `PARITY_V2_MATCH`. The verifier now requires `--input` and delegates match-producing verification to the exact three-file pair verifier. An adversarial regression changes settlement semantics and recomputes both hashes; verification fails against the unchanged Hummingbot economics.

The canonical quants-lab producer contract uses `ordered_fill_ids`, per-fill `fill_timestamps_ns`, endpoint keys `first_fill_timestamp_ns`/`last_fill_timestamp_ns`, timestamp identity, settlement-specific terminal outcomes, and eight all-false authority fields. The Hummingbot parser consumes those exact names while independently deriving its own eight-key all-false result authority.

## Authority boundary

Every artifact, result, verifier, and pair object carries all-false authority. The v2 code contains no connector, executor, order, credential, account, service, or deployment operation.

## Validation commands

```bash
python -m pytest -q \
  test/rsi_v5/test_perp_episode_accounting.py \
  test/rsi_v5/test_perp_episode_parity_io.py \
  test/rsi_v5/test_perp_episode_parity_verify.py \
  test/rsi_v5/test_perp_episode_parity_pair.py \
  test/rsi_v5/test_perp_episode_parity_v2.py

python -m compileall -q \
  controllers/directional_trading/rsi_v5_core \
  scripts \
  test/rsi_v5

conda run -n hummingbot pre-commit run --files <touched-files>
```

These checks establish repository-local contract behavior only. They do not establish quants-lab producer parity, paper/canary readiness, execution quality, alpha, profitability, or promotion.
