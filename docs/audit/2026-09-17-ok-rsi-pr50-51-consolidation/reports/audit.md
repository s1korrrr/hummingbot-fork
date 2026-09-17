# Re-audit after t16

Gate: **PASS**

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
HEAD: `4a4372bcde4990355421a42df3b8ce6107299abb` (worker edits are uncommitted; this audit ran the working tree)
Python: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python` (conda env `hummingbot`, 3.13.13)
Did not run Docker, live stack, exchanges, or CI. Did not edit product or test code. Did not concatenate the pytest union.

t15 Important (live / `SHUTTING_DOWN` hold consuming a prior bag via report order) is closed in this tree and tested. t16 residual (already-TERMINATED freeze with the prior claimant dropped still matching a leftover-sized bag) is cannot-verify, not a gate fail. No test proves premature release of a live hold.

## How this repo tests and lints

Existing stack, not invented:

- Pytest: `Makefile` target `test` runs `python -m coverage run -m pytest --import-mode=importlib` with connector ignores. `pyproject.toml` only sets `asyncio_default_fixture_loop_scope = "function"`.
- Lint: `.flake8` (max-line-length 120, ignores E251/E501/E702/W503/W504) and the pre-commit `flake8` hook. flake8 7.3.0 in the hummingbot env.
- This worktree’s Cython `*.so` modules are present (60 files, gitignored copies). Collection succeeded.

Focused suites below are what the t14/t17 briefs required. Not `make test`. Split invocations only.

## Commands

| Invocation | Exit | Summary |
| --- | --- | --- |
| `pytest --import-mode=importlib -q test/test_ok_rsi_down_only_entry_execution.py` | 0 | 65 passed in 1.90s |
| `... test/hummingbot/core/data_type/test_in_flight_order.py test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py` | 0 | 36 passed, 1 warning (deprecated `get_event_loop`) in 1.09s |
| `... test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py` | 0 | 22 passed in 2.96s (was 18 before t16) |
| `... test/hummingbot/strategy_v2/executors/position_executor/` | 0 | 110 passed, 10 subtests passed in 3.33s |
| `... test/test_ok_rsi_down_only_entry_factory.py` | 0 | 36 passed in 2.78s |
| `flake8` on the six touched product files | 0 | no output |

Named E / t16 tests (not a substitute for the split suite):

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest --import-mode=importlib -v \
  -k 'test_position_hold_without_held_bag_refuses_new_buy or test_position_hold_undersized_or_prior_bag_refuses_new_buy or test_new_active_hold_before_stored_prior_hold_refuses_new_buy or test_new_shutting_down_hold_before_stored_prior_hold_refuses_new_buy or test_new_hold_without_prior_claimant_refuses_leftover_sized_bag' \
  test/test_hl_rsi_entry_control.py test/test_ok_rsi_admission_safety.py
```

Exit: **0**. Collected 22 / selected 8 / **8 passed, 14 deselected in 2.86s**.

- `test/test_hl_rsi_entry_control.py::test_position_hold_without_held_bag_refuses_new_buy` PASSED
- `test/test_hl_rsi_entry_control.py::test_position_hold_undersized_or_prior_bag_refuses_new_buy` PASSED
- `test/test_hl_rsi_entry_control.py::test_new_active_hold_before_stored_prior_hold_refuses_new_buy` PASSED
- `test/test_hl_rsi_entry_control.py::test_new_hold_without_prior_claimant_refuses_leftover_sized_bag` PASSED
- `test/test_ok_rsi_admission_safety.py::test_position_hold_without_held_bag_refuses_new_buy` PASSED
- `test/test_ok_rsi_admission_safety.py::test_position_hold_undersized_or_prior_bag_refuses_new_buy` PASSED
- `test/test_ok_rsi_admission_safety.py::test_new_shutting_down_hold_before_stored_prior_hold_refuses_new_buy` PASSED
- `test/test_ok_rsi_admission_safety.py::test_new_hold_without_prior_claimant_refuses_leftover_sized_bag` PASSED

Conflict markers (`<<<<<<<` / `>>>>>>>`) in `controllers/`, `hummingbot/`, and `test/`: none.

`_held_buy_inventory_observed()` is absent from `hl_rsi.py`.

Combined working tree vs HEAD: 14 files, +2604/−305. Mixed `MM`/`AM` remains.

## Spec items

### A. Delayed OPEN cannot revive CANCELED/FILLED — implemented

`InFlightOrder.update_with_order_update` skips reopen states. TradeUpdate path unchanged. FILLED after CANCELED still applies.

Tests (isolated native/OKX run, 36 passed): `test_update_with_order_update_does_not_reopen_canceled_order`, `..._filled_order`, `..._still_copies_exchange_id_and_cancel_source_on_refused_open`, `..._filled_after_canceled_still_applies`, `test_update_with_trade_update_still_applies_after_canceled`, OKX `test_cached_canceled_order_does_not_revive_on_delayed_open`, `test_request_order_status_delayed_live_does_not_reopen_canceled_order`.

### B. Source 32 beats bound reprice — implemented

`process_order_canceled_event` classifies `"32"` before `_replacement_bound_to`. Test: `test_stp_cancel_source_does_not_place_bound_reprice_child` (in the 65 passing down-only tests).

### C. Deadline while PENDING_CREATE; cooldown at child submit — implemented

Wait states call `_revocation_or_deadline_reason()`. Tests: `test_pending_create_expires_intent_without_dropping_identity`, `test_reprice_child_honors_cooldown_at_submit`, `test_failed_cancel_retries_after_intent_deadline_and_blocks_replacement`.

### D. Non-RUNNING owner and data_health unreadiness refuse child — implemented

`_live_entry_permission` refuses present non-RUNNING `status` and present `data_health.ready` false/missing. Tests: `test_stopped_owner_cannot_place_replacement_after_recon`, `test_data_health_unreadiness_blocks_child_despite_cached_quotes`.

### E. covering-sum leftover vs bag; live/SHUTTING_DOWN holds do not consume bag — implemented

`accounting_owner=held_position` or `CloseType.POSITION_HOLD` is not a release by itself. `_held_buy_inventory_observed()` (any positive same-pair BUY row) is gone.

`_executor_hold_released_to_bag` now:

- returns False unless the claimant is done (`_executor_is_done`; `ExecutorInfo.is_done` is `status == TERMINATED`)
- live and `SHUTTING_DOWN` holds therefore never consume `positions_held`
- among done hold claimants only, oldest-first by `(close_timestamp or timestamp, id)`; missing/duplicate age keys fail closed
- prefix covering-sum of those leftovers vs `_held_buy_inventory_amount()`; unmeasurable leftover fails closed

Until covering succeeds for that claimant:

- leftover stays on the buy-capital walk
- owned and outstanding are `None`
- valuation is Infinity
- `can_create_executor` refuses with `exposure_unavailable`

Prefix over raw `executors_info` / `_filter_same_side` report order is gone.

t16 cases that ran in both capital files:

- empty bag + live / `SHUTTING_DOWN` hold → refuse `exposure_unavailable`
- leftover `0.4` + bag `0.1` → refuse
- two done holds leftover `0.4` each + bag `0.4`, listed newest-first → older owned `0`, newer unmatched, refuse
- new live / `SHUTTING_DOWN` hold listed before a stored prior done hold, bag covers one leftover → new unmatched, prior may consume the bag, refuse
- new live / `SHUTTING_DOWN` hold, prior claimant absent, leftover-sized bag → new unmatched, refuse

hl_rsi covers the live stub (`is_active=True`, `is_done=False`). ok_rsi covers real `RunnableStatus.SHUTTING_DOWN` `ExecutorInfo` snapshots. Same release branch: not done, so bag is not consumed.

Kept: managing_fill 0.1/0.9/100, TERMINATED leftover + bag of the same size = 40 (no double count), missing/conflict/over-authorized/non-finite owned → Infinity.

t16 residual (later already-TERMINATED freeze with the prior done claimant dropped still matching a leftover-sized bag): **cannot-verify**. Same snapshot as the KEEP “TERMINATED leftover + bag = 40” case. Bag rows have no executor id. Do not fail the gate on that alone. No test proves premature release of a live / `SHUTTING_DOWN` hold.

### F. Next quantized close qty for flat fees — implemented

Tests in the 110 that passed: `test_residual_quantized_close_amortizes_flat_fee_over_next_sell_quantity`, `test_flat_fee_floor_uses_remainder_after_prior_close`, `test_blocked_close_quantity_holds_without_inventing_a_fee_rate`.

### G. cancel_source JSON round-trip — implemented

`to_json` / `from_json` persist a non-blank source. Tests: `test_canceled_cancel_source_json_round_trip_is_not_open`, `test_from_json_missing_or_blank_cancel_source_is_none`.

### Overlap file has no conflict markers — implemented

Down-only suite is the merged union (65 tests).

### Docker / live / CI — cannot verify (not run)

Not claimed.

## Gate

PASS. Every A–G item has tests. Split required suites and flake8 exited 0. Empty-bag, undersized/prior-bag, and both t16 live/`SHUTTING_DOWN` vs stored-bag cases exist in the two capital files, ran, and passed. New behavior (not-done holds do not consume `positions_held`) is tested.

Concatenated union was not re-run. Treat it as a pytest isolation hazard if someone later runs those files in one process after asyncio tests.

## Gaps (residual, not gate-fail)

- Later already-TERMINATED freeze with the prior done claimant dropped from `executors_info` still matches a leftover-sized bag. Same snapshot as TERMINATED leftover + bag = 40. No executor id on bag rows. Live / `SHUTTING_DOWN` windows are closed and tested. Dropped-done later freeze is untested. Cannot-verify; no test proves premature release of a live hold.
- hl_rsi t16 cases use live `is_active` / `is_done` stubs, not `RunnableStatus.SHUTTING_DOWN`. ok_rsi covers SHUTTING_DOWN on `ExecutorInfo`. Product capital walk uses `executors_info`; `ExecutorInfo.is_done` is TERMINATED-only.
- `_executor_is_done` falls back to `not is_active` when `is_done` is missing. `ExecutorBase.is_active` is False in SHUTTING_DOWN. Product path still uses `ExecutorInfo`. Residual only if helpers ever receive live executor objects.
- Covering identity is still `is`. Age order uses executor `id` only as a tiebreak.
- Concatenated pytest union: historically 23 `InFlightOrderPyUnitTests` setup errors via `asyncio.get_event_loop()` after pytest-asyncio files. Split invocations pass. Not re-run this re-audit.
- OKX WS `_user_stream_event_listener` looks up `all_updatable_orders`, which skips the TTL cache. A delayed WS `live` after cancel never reaches `update_with_order_update`. A/G tests hit REST / `process_order_update` on the cached object.
- Missing `controller.status` still counts as allowed so default `bind_controller` keeps shared tests `risk_allowed=True`. D covers present non-RUNNING statuses.
- Absent `data_health` still allows a child. Present `ready=False` is covered.
- Native `FAILED` is not a reconciled remainder. Conservative: keeps capital reserved.
- `InFlightOrder.attributes` / `__eq__` still omit `cancel_source`. JSON round-trip is the persistence check.
- `_close_order_amount` swallows quantize exceptions and returns the unquantized cap. F tests mock `quantize_order_amount` as identity.
- This worktree is not a compiled Cython build. Pytest used gitignored `*.so` copies (60 present). A clean clone without those modules will fail collection.
- Cooldown wait leaves `_entry_child_submission_authorized` True (tested). A later tick with elapsed cooldown will place.
- Working tree remains mixed `MM`/`AM`. Combined contents vs base are what was tested. A commit that stages only the index would drop unstaged hunks.
