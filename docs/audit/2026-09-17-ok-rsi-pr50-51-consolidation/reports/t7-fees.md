# t7 — Residual close fees (F)

Status: **DONE**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation` (HEAD still `4a4372bcd`)
Git: no commit, no push, no PR. PR51 fee helpers were already staged by t4 (`M `). F edits are unstaged on top (`MM` on executor + floor tests).

Authority: `repository-process-only`. Did not touch `/Users/s1kor/dev/trading/rsibot/hummingbot`, `down_only_entry_execution.py`, or `hl_rsi.py`.

PR51 `position_executor.py` was present (not base-only). Implemented F on that checkout.

## F

`_expected_close_fee_rate` now asks `_close_order_amount()` for the next sell quantity (that helper already delegates to `_single_attempt_spot_close_amount` when policy/resume requires it).

- `close_qty is None` or `<= 0` → return `None` (unknown/unavailable hold). Does not invent a rate.
- `get_fee` / `build_trade_fee` / `_fee_rate_from_trade_fee` take `close_qty` and `entry_price`.
- LIMIT/taker classification unchanged: `is_maker` only for `LIMIT_MAKER`.
- Unknown-fee hold in `_fee_rate_from_trade_fee` unchanged (third-token, non-finite, rate not in `[0, 1)`).
- Entry inventory conservation unchanged: `open_filled_amount` still subtracts buy base fees.
- No-loss formula unchanged: `entry * (1 + known_entry_fee) / (1 - sell_fee_rate)`, with `sell_fee_rate` now amortizing quote/base flats over `close_qty * entry_price`.
- Place-close still raises the limit to `max(price, min_sell)` and holds/early-stops when current is below the floor or the floor is unknown.

## Tests

New behavior in `test_trailing_no_loss_floor.py`:

- Original fill `1.0`, next sell `0.01` (available-balance cap), same USDT flat `0.05` → rate is `flat / (0.01 * 100)`, not `flat / (1 * 100)`. Price `102` sits between the two floors and holds (no sell).
- Original fill `1.0`, `0.9` already closed, remainder `0.1` → floor uses `flat / (0.1 * 100)`. Price `100.40` holds.
- `_close_order_amount()` returns `0` → fee rate `None`, trailing close holds.

Existing PR51 LIMIT/taker and full-lot flat-fee tests still pass (close qty equals original fill there).

Command:

```bash
cd /Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/hummingbot/strategy_v2/executors/position_executor/ --import-mode=importlib -q
```

Result: **110 passed, 10 subtests passed in 4.23s** (conda env `hummingbot`). flake8 on the three owned files: clean. Not native CI/Docker.

## Files changed

Edited:

- `hummingbot/strategy_v2/executors/position_executor/position_executor.py`
- `test/hummingbot/strategy_v2/executors/position_executor/test_trailing_no_loss_floor.py`

Owned, unchanged by t7 (PR51 checkout only):

- `test/hummingbot/strategy_v2/executors/position_executor/test_position_executor.py`

## Concerns

1. Asking `_close_order_amount()` from the fee helper can log the existing available-balance cap warning before a close is placed. Same helper place-close already calls.
2. Retry-path `_close_order_amount` still returns unquantized `amount_to_close` when available balance is `None` (pre-existing). F uses that quantity rather than inventing a block.
3. The blocked-quantity test uses retry + available `0`, which is the same `close_qty <= 0` hold branch single-attempt `blocked_reason` takes. Did not add a separate single-attempt residual-below-minimum fixture.
