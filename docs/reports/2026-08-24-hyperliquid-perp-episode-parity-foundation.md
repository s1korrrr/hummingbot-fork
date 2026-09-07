# Hyperliquid Perpetual Episode Parity Foundation

**Date:** 2026-08-24
**Branch:** `feat/andrzej_hyperliquid_rl_episode_parity_v3`
**Status:** read-only implementation candidate; latest-head execution verification pending
**Capital verdict:** `HOLD_FOR EVIDENCE`

## Delivered

- exact linear-perpetual account snapshots using collateral rather than spot-style inventory cash;
- long positions marked to executable bid and shorts to executable ask;
- entry-price unrealized PnL, contract multipliers, accrued funding, close-fee reserve, and close-impact reserve;
- fractional/residual-position accounting;
- `force_flat`, `carry_state`, and `mark_to_liquidation` terminal semantics;
- liquidatable-NAV reward with external-cashflow neutrality and exact transition attribution reconciliation;
- quants-lab expectation and Hummingbot actual comparison with deterministic tolerances;
- `MATCH`/`MISMATCH` results whose mismatch set is recomputed from the numbers and cannot be forged by editing status fields;
- strict canonical decimal JSON input and atomic fsynced result publication;
- persisted-result verification reconstructing actual, expectation, authority, breakdown, and top-level hashes;
- input/result pair verification that re-evaluates the exact input and requires canonical equality with the persisted result;
- distinct command outcomes for valid match, valid mismatch, and invalid/tampered evidence.

## Commands

```bash
python scripts/run_perp_episode_parity.py \
  --input <strict-input.json> \
  --output <result.json>

python scripts/verify_perp_episode_parity.py \
  --result <result.json>

python scripts/verify_perp_episode_parity_pair.py \
  --input <strict-input.json> \
  --result <result.json>
```

The runner publishes both `MATCH` and `MISMATCH` as valid evidence. The result verifier and pair verifier return `0` only for `MATCH`, `2` for a valid `MISMATCH`, and `1` for invalid, tampered, non-canonical, or provenance-mismatched evidence.

## Authority boundary

This slice consumes reconciled snapshots and produces read-only accounting/parity evidence. It does not:

```text
create an executor
create an order
cancel an order
mutate account state
choose leverage
choose notional
approve Risk
approve capital
start paper/canary/live trading
```

All paper, canary, live, capital, Risk, execution, order, and trading authority remains false.

## Verification truth

The implementation and regression tests are committed, but the current ChatGPT environment cannot clone or execute the private repository. Therefore this report does **not** claim latest-head pytest, formatting, type-check, or runtime green.

Required commands:

```bash
conda run -n hummingbot python -m pytest -q \
  test/rsi_v5/test_perp_episode_accounting.py \
  test/rsi_v5/test_perp_episode_parity_io.py \
  test/rsi_v5/test_perp_episode_parity_verify.py \
  test/rsi_v5/test_perp_episode_parity_pair.py

pre-commit run --files \
  controllers/directional_trading/rsi_v5_core/perp_episode_accounting.py \
  controllers/directional_trading/rsi_v5_core/perp_episode_parity_io.py \
  controllers/directional_trading/rsi_v5_core/perp_episode_parity_result.py \
  controllers/directional_trading/rsi_v5_core/perp_episode_parity_pair.py \
  scripts/run_perp_episode_parity.py \
  scripts/verify_perp_episode_parity.py \
  scripts/verify_perp_episode_parity_pair.py \
  test/rsi_v5/test_perp_episode_accounting.py \
  test/rsi_v5/test_perp_episode_parity_io.py \
  test/rsi_v5/test_perp_episode_parity_verify.py \
  test/rsi_v5/test_perp_episode_parity_pair.py

python -m compileall -q \
  controllers/directional_trading/rsi_v5_core \
  scripts \
  test/rsi_v5
```

No real Hummingbot account capture or quants-lab campaign artifact has been compared by this PR.
