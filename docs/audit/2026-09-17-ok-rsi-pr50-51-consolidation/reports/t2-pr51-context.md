# t2 — PR #51 context packet

Read-only inspect of worktree `hummingbot-entry-control-consistency`.

- Head: `d38705b0813a6e39747908cbce74f1f08015e0dc` (`feat/andrzej_ok_rsi_entry_control_consistency`)
- Base: `4a4372bcde4990355421a42df3b8ce6107299abb`
- Commit: `fix(ok-rsi): keep pause, capital, and trailing semantics honest`
- Authority: repository-process-only. No product edits.

PR #51 already wires live pause/kill/signal into the maker snapshot, stops treating operator pause as `POSITION_HOLD`, treats `managing_fill` as a capital-terminal label, and amortizes quote/base flats into the no-loss floor while classifying ordinary LIMIT as taker. Blockers D/E/F are not closed. The remaining holes are owner-status and book-evidence at child submit, TERMINATED/receipt valuation, and fee notional vs residual close quantity.

## 1. Diffstat vs base

`git diff --stat 4a4372bc...HEAD`

```
 controllers/directional_trading/hl_rsi.py          | 401 +++++++++++++--------
 controllers/directional_trading/ok_rsi.py          |   4 +-
 .../ok_rsi_core/down_only_entry_execution.py       | 132 ++++---
 .../position_executor/position_executor.py         |  39 +-
 .../position_executor/test_position_executor.py    |  66 +++-
 .../test_trailing_no_loss_floor.py                 |  55 ++-
 test/test_hl_rsi_entry_control.py                  | 238 ++++++++++++
 test/test_ok_rsi_admission_safety.py               |  33 ++
 test/test_ok_rsi_down_only_entry_execution.py      | 173 ++++++++-
 test/test_ok_rsi_down_only_entry_factory.py        |  38 +-
 10 files changed, 966 insertions(+), 213 deletions(-)
```

Overlap with PR #50: `down_only_entry_execution.py` and `test_ok_rsi_down_only_entry_execution.py`.

## 2. Blockers D, E, F

### D. P1 — Stopped controller or unusable data still allows a child

Spec: check current owner state and venue/pair execution-data at the final child boundary. Do not rerun one-shot parent admission. Do not abandon owned fills when entry inputs fail.

| Function | Lines | Checked | Missing |
| --- | --- | --- | --- |
| `DownOnlyMakerPositionExecutor._live_book` | `down_only_entry_execution.py` 235–243 | `strategy.current_timestamp`; `get_price(connector, pair, BestBid/BestAsk)` | Finite/positive quotes; bid < ask (locked/crossed is later, in `decide_down_only_maker`); book last-update vs strategy clock; connector ready / pair subscribed; cached last quote vs live |
| `DownOnlyMakerPositionExecutor._live_entry_permission` | 245–255 | `strategy.controllers` is a dict; controller exists for `config.controller_id`; `processed_data.signal < 0`; `config.manual_kill_switch`; `_operator_entry_paused` | `controller.status` / `is_active` (STOPPED/TERMINATED still allow a child if leftover signal ≥ 0); `data_health.ready`; processed_data timestamp freshness; connector/pair identity (lookup is controller_id only) |
| `DownOnlyMakerPositionExecutor._current_maker_decision` | 257–290 | Live book + permission into `decide_down_only_maker`; on exception, terminal cancel `input_unavailable` | Same owner/data gaps as the two helpers |
| `DownOnlyMakerPositionExecutor._submission_is_valid` | 475–514 | Re-runs maker decision with live permission/book; `target >= ask` → `submission_would_cross`; any exception → `input_unavailable` | Owner RUNNING / data_health (via `_live_entry_permission`); on fail always `_end_entry_acquisition`, `state=ended_no_fill`, `CloseType.EXPIRED`, `stop()`. Child path currently skips this when `gross_filled > 0` (414–424), so owned fills are not abandoned on this path today, but the helper itself is not fill-safe |
| `DownOnlyMakerPositionExecutor._submit_reprice_child` | 399–473 | Price/policy/open-order/reprice-count; **then** gross fill → `managing_fill`; **then** `_submission_is_valid` | No extra owner-status or data_health check beyond `_submission_is_valid` |
| `OKRSIController.stop_actions_proposal` | `ok_rsi.py` 281–316 | Kill, opposing signal, shadow-input failure → `StopExecutorAction(keep_position=True)` | Operator pause is intentionally excluded (new BUY is refused in `can_create_executor`). Stopped controller is not a child-boundary check |
| `HLRSIController.validate_executor_config` / `can_create_executor` | `hl_rsi.py` 4399–4514 | Parent admission: `data_health.ready`, kill, pause, exposure | Not invoked for replacement children. Spec forbids rerunning this one-shot gate |

`decide_down_only_maker` (`ok_rsi_core/down_only_entry.py` 149–159) treats `risk_allowed=False` as `CANCEL_TERMINAL` / `RISK_BLOCKED`. That is the right hook for owner unreadiness. `_live_entry_permission` currently maps only missing controller, kill, and pause onto `risk_allowed=False`. A stopped controller that is still in `strategy.controllers` with stale `signal=1` returns `(False, True)` and a child can submit.

`get_price` (`executor_base.py` 317–326) is `connectors[name].get_price_by_type(pair, type)`. That is last connector quote, not a freshness proof. Tests mock `get_price` with a dict; they never mark the book unready while leaving cached bid/ask intact. `locked_book` (equal bid/ask) is the only book-quality revoke, and it surfaces as `input_unavailable` because `_validate` raises on a locked book (`down_only_entry.py` 122–123).

### E. P1 — Premature or unavailable capital release

Spec: `hl_rsi.py` acquisition helpers. Separate owned inventory, unresolved acquisition, and report labels. Release only reconciled commitment. Reject missing / non-finite / conflicting / over-authorized receipts.

| Function | Lines | Checked | Missing |
| --- | --- | --- | --- |
| `_TERMINAL_ENTRY_LIFECYCLE_STATES` | `hl_rsi.py` 92–96 | `managing_fill`, `ended_no_fill`, `held_residual` | `early_stop` / `POSITION_HOLD` / `stop_reason` labels; `RunnableStatus.TERMINATED`; `execution_receipt.accounting_owner` |
| `_executor_entry_lifecycle_state` | 1440–1447 | `entry_acquisition.state` else `entry_lifecycle.state` | Status; receipt; empty state is `None` |
| `_executor_acquisition_may_still_fill` | 1449–1450 | `state not in` the three labels. `None` counts as still-fillable | TERMINATED owner; `early_stop` without rewriting state |
| `_executor_owned_base` | 1452–1469 | First finite `remaining_position_amount_base` or `execution_receipt.remaining_owned_base`; else `filled - closed`; missing filled → **0** | Missing receipt as unavailable (`None` / Infinity); conflict among remaining vs receipt vs filled−closed; owned > authorized; `accounting_owner=held_position` vs still counting executor |
| `_executor_outstanding_base` | 1471–1490 | If not may-still-fill → `0`. Else `max(0, authorized - max(open_filled, acquisition cumulative))` | Unreconciled working order after `managing_fill`; TERMINATED-but-untransferred remainder; filled > authorized is clamped to 0 outstanding, not rejected |
| `_get_active_buy_executor_position_value_usd` | 1549–1563 | Sum `owned * mid` + `outstanding * max(mid, working/ceiling)` over `_filter_same_side(BUY, active_only=True)`. Non-finite owned/outstanding/mid → Infinity | TERMINATED executors (`is_done`) drop out even if `remaining_owned_base` is still on the executor and not in `positions_held` |
| `_filter_same_side(..., active_only=True)` | 582–591 | Uses `not is_done` when present, else `is_active` | `SHUTTING_DOWN` stays (`is_done` false). `TERMINATED` leaves. Labels are ignored here |
| `PositionExecutor.early_stop` | `position_executor.py` 1392–1404 | Sets `POSITION_HOLD` or `EARLY_STOP` and `SHUTTING_DOWN` if not already closed | Does not set lifecycle `managing_fill` / `ended_no_fill` |
| `PositionExecutor._end_entry_acquisition` | 937–949 | Latches `stop_reason` + timestamps only | Does not change `state` |
| `DownOnlyMakerPositionExecutor.early_stop` | `down_only_entry_execution.py` 226–233 | With `keep_position`, latches `external_invalidation` then parent `early_stop` | Same: label in `stop_reason`, not in terminal state set |

`managing_fill` effect on valuation: outstanding commitment is released (`_executor_outstanding_base` → 0). Owned base is still counted. Spacing/status price switches from working order to remaining-position basis (`_executor_spacing_ref_price` 1544–1547, `_status_executor_price` 5094–5102). That matches “report labels vs owned inventory” for a **running** owner after recon.

`early_stop` labels do **not** enter `_TERMINAL_ENTRY_LIFECYCLE_STATES`. A `SHUTTING_DOWN` + `POSITION_HOLD` owner with lifecycle still `open`/`resting` still reserves outstanding (filter keeps it because `is_done` is false). That is conservative. The opposite hole is the one the spec names: a `TERMINATED` owner with leftover `remaining_owned_base` is invisible to `_get_active_buy_executor_position_value_usd`, so capital is released before hold transfer.

Missing filled on a still-active executor is valued as owned 0 (`_executor_owned_base` 1466–1467), not unavailable. That is silent understatement, not Infinity.

### F. P2 — Residual flat-fee economics

Spec: estimate actual next quantized close quantity. Preserve no-loss interpretation, base-fee inventory conservation, unknown fee evidence, LIMIT/taker classification.

| Function | Lines | Checked | Uses original `open_filled_amount`? |
| --- | --- | --- | --- |
| `open_filled_amount` | `position_executor.py` 249–259 | Gross entry fills minus buy base fees | This **is** the original owned lot, not the next close lot |
| `amount_to_close` | 272–278 | `open_filled_amount - close_filled_amount` | Residual inventory, still not exchange-quantized |
| `_single_attempt_spot_close_amount` | 313–361 | Floor to quantum, min size/notional, available-balance cap, `quantize_order_amount` | Actual next sell quantity lives here |
| `_close_order_amount` | 280–311 | Single-attempt/resume → `_single_attempt_spot_close_amount`; else min(amount_to_close, available) quantized | Used at place-close time only, not for the floor |
| `_expected_close_fee_rate` | 476–512 | `get_fee` / `build_trade_fee` with `order_type=close_order_type`, `is_maker = (LIMIT_MAKER only)` | **Yes. `amount=self.open_filled_amount`, `price=self.entry_price`** |
| `_fee_rate_from_trade_fee` | 514–544 | percent + quote flats + base flats × price; third-token / non-finite / rate ∉ [0,1) → None (hold) | **Yes. Callers pass `self.open_filled_amount`. Flat is `extra / (amount * price)`** |
| `_min_no_loss_sell_price` | 546–555 | `entry * (1 + known_entry_fee) / (1 - sell_fee_rate)` | Sell rate from original fill notional. `_known_fee_pct` also uses `open_filled_amount_quote` (466–467) |
| `_should_hold_trailing_close` / place-close floor | 571–578, 1225–1241 | Hold if min_sell unknown or current < min_sell; limit price `max(price, min_sell)` | Floor is computed on original fill, then applied to the quantized close amount |

PR #51 correctly stopped treating age-0 LIMIT as maker and added flat-fee amortization. It did not switch the notional to the next quantized close quantity. A dust remainder (or quantum-capped sell) still spreads a flat as if the original fill were sold. That understates the rate and can sell through the no-loss floor. Unknown fees still hold. Base-fee conservation on **entry** (`open_filled_amount` subtracts base fees) is unchanged and should stay.

## 3. `_live_entry_permission`, `_live_book`, `_submission_is_valid`

### Signatures

```python
def _live_book(self):  # -> tuple[Decimal, Decimal, Decimal]  (now, best_bid, best_ask)
def _live_entry_permission(self) -> tuple[bool, bool]:  # (opposing_signal, risk_allowed)
def _submission_is_valid(self, target: Decimal) -> bool:
```

All three are methods on `DownOnlyMakerPositionExecutor` in `controllers/directional_trading/ok_rsi_core/down_only_entry_execution.py`. No other production call sites.

### Call sites

| Caller | Lines | When |
| --- | --- | --- |
| `control_open_order` → `_submission_is_valid(self._entry_current_price)` | 142 | First parent submit only (`tracked is None` and not yet submitted) |
| `_current_maker_decision` → `_live_book` + `_live_entry_permission` | 258–259 | Every resting/reprice control tick, including cancel-pending (184–200) |
| `_submit_reprice_child` → `_submission_is_valid(target)` | 425 | After recon, only if `gross_filled == 0` |
| `_submission_is_valid` → `_live_book` + `_live_entry_permission` | 478–479 | Final child/parent submit gate |

`control_open_order` 139–147 is the other submit path. It does not re-check controller RUNNING or `data_health`.

## 4. `managing_fill` / `early_stop` vs `_executor_acquisition_may_still_fill` and valuation

```92:96:controllers/directional_trading/hl_rsi.py
_TERMINAL_ENTRY_LIFECYCLE_STATES = frozenset({
    "managing_fill",
    "ended_no_fill",
    "held_residual",
})
```

```1449:1450:controllers/directional_trading/hl_rsi.py
    def _executor_acquisition_may_still_fill(self, executor) -> bool:
        return self._executor_entry_lifecycle_state(executor) not in _TERMINAL_ENTRY_LIFECYCLE_STATES
```

- `managing_fill`: may-still-fill is false. Outstanding quote is released. Owned remaining is still reserved at mid. Working-order price is ignored for spacing.
- `early_stop`: sets executor `SHUTTING_DOWN` and `CloseType.POSITION_HOLD`/`EARLY_STOP`. `_end_entry_acquisition` writes `stop_reason` only. Unless something else already set `managing_fill`/`ended_no_fill`/`held_residual`, may-still-fill stays true and SHUTTING_DOWN remains in the active filter (`is_done` is false until TERMINATED).
- `RunnableStatus.TERMINATED`: `ExecutorInfo.is_done` true (`executors_info.py` 40–41). Dropped from `_filter_same_side(active_only=True)` regardless of leftover receipt. Capital and owned inventory on that object vanish from `_get_active_buy_executor_position_value_usd`.
- `is_active` (`executor_base.py` 93–97) is RUNNING/NOT_STARTED only. Valuation does not use that field when `is_done` exists.

## 5. Close-fee helpers and original `open_filled_amount`

Yes. All three fee-floor helpers key off original fill, not next quantized close:

```486:512:hummingbot/strategy_v2/executors/position_executor/position_executor.py
            candidate = get_fee(
                ...
                amount=self.open_filled_amount,
                price=self.entry_price,
                is_maker=is_maker,
            )
        ...
                    self.open_filled_amount,
                    self.entry_price,
        ...
        return self._fee_rate_from_trade_fee(fee, self.open_filled_amount, self.entry_price)
```

`_fee_rate_from_trade_fee` is amount-generic, but the only production caller passes `open_filled_amount`. `_min_no_loss_sell_price` uses that rate plus `_known_fee_pct()` over `open_filled_amount_quote`.

Place-close then sells `_close_order_amount()` (quantized residual) at a floor computed as if the original lot were the close.

## 6. Existing tests that miss the holes

| Hole | Tests that come close | What they actually assert |
| --- | --- | --- |
| TERMINATED owner still holding inventory | `test_ok_rsi_admission_safety.py`: `test_pending_entry_reserves_its_full_commitment_including_during_shutdown` (SHUTTING_DOWN, 33–45); `test_terminally_canceled_remainder_releases_only_resolved_commitment` (`managing_fill` while RUNNING, 48–57); `test_completed_loss_is_counted_before_any_entry_proposal` (TERMINATED, 89–99) | Shutdown still reserves. `managing_fill` releases remainder on a live owner. TERMINATED is only used for consecutive-loss pause, not capital |
| Unreadiness with cached quotes | `test_ok_rsi_down_only_entry_execution.py`: `test_replacement_does_not_submit_after_permission_or_book_revoked` (`pause`/`kill`/`opposing`/`locked_book`, 831–857); `test_unbound_controller_cannot_place_replacement_after_recon` (813–827); factory `test_active_entry_invalidation_emits_keep_position_stop` (`unavailable` sets `BestAsk=None`, 198–225) | Pause/kill/signal and equal bid/ask or missing controller. No STOPPED owner. No `data_health.ready=False` with bid/ask still populated. `bind_controller` (150–157) never sets `status` |
| Residual-quantity fees | `test_trailing_no_loss_floor.py`: full fill `amount=1` (27–71, 138–157); `test_position_executor.py` trailing close still sells amount `1` (688–709) | Flat is `SELL_FLAT_QUOTE / ENTRY` with ENTRY=100 and amount=1. No quantum-capped remainder |
| Missing / conflicting / over-authorized receipts | `test_invalid_inventory_cannot_be_valued_as_zero_exposure` (81–86) is **positions_held** `amount='bad'` → Infinity. `test_transferring_owned_inventory_between_native_owners_preserves_buying_capacity` (60–78) has a complete fill | No executor with absent `remaining_position` + absent receipt + absent filled. No remaining vs receipt mismatch. No owned > `config.amount` |

Other PR #51 tests that are useful but orthogonal: pause does not `POSITION_HOLD` a filled trailer (`test_operator_pause_keeps_filled_trailing_owner_running`, factory `test_operator_pause_does_not_stop_a_filled_trailing_owner_and_refuses_new_buy`); LIMIT is taker for age 0 and 3 (`test_ordinary_limit_close_fee_is_not_maker_for_age_zero_and_three`); drawdown/spacing use remaining basis (`test_hl_rsi_entry_control.py`).

## 7. Smallest patch (no implementation)

Keep one writer on the overlap files. Do not reopen parent `can_create_executor` on the child path.

1. **D — owner + evidence at `_live_entry_permission` / `_live_book` only.** Treat controller not RUNNING (and missing `data_health.ready`) as `risk_allowed=False`. In `_live_book`, require finite positive bid < ask; if the connector has no current book for that pair, raise so existing `input_unavailable` / `risk_blocked` paths fire. Do not invent a second admission stack. In `_submission_is_valid`, if `cumulative_filled_base > 0`, refuse the child and keep `managing_fill` instead of `stop()`/`EXPIRED`. Tests: STOPPED controller with stale signal=1 and still-good cached bid/ask must not place a child; `data_health.ready=False` with cached quotes must not place a child.

2. **E — value owned until a hold receipt exists.** Keep `managing_fill` as “no more acquisition.” Include non-active executors that still report `remaining_owned_base > 0` and `accounting_owner != held_position` in exposure. If remaining/receipt/filled−closed disagree, or filled/remaining is missing/non-finite, return Infinity (same as bad `positions_held`). Test TERMINATED + leftover remaining vs TERMINATED + `positions_held` transfer. Do not teach `_executor_owned_base` to treat missing filled as 0.

3. **F — pass next close quantity into the existing helper.** `_expected_close_fee_rate` should call `_fee_rate_from_trade_fee(fee, close_qty, price)` where `close_qty` is the quantized amount `_close_order_amount` / `_single_attempt_spot_close_amount` would sell (or hold if that helper blocks). Leave LIMIT/taker and None-hold as they are. One test: original fill 1.0, next sell 0.01, same quote flat → floor uses `flat / (0.01 * price)`, and a price between the two floors holds.

Rollback: revert the three-file slice (`down_only_entry_execution.py`, `hl_rsi.py` helpers, `position_executor.py` fee amount argument). Pause-vs-kill and LIMIT-taker classification can stay.

## Concerns

- Child submit still trusts stale controller membership + cached `get_price_by_type` quotes.
- `managing_fill` is a report label that releases remainder while the owner is RUNNING; `early_stop` does not use that label; `TERMINATED` drops owned inventory from valuation.
- Flat-fee floor still amortizes over original `open_filled_amount`.
- Overlap files with PR #50: any D fix in `down_only_entry_execution.py` must not fight the post-only cancel/reprice work.
