# OK RSI repair publication audit

The original repair was developed against `91372522a8e4bb178a959f1b0c42ec7276cf89ba`. Publication integrates it with primary-branch commit `6647f366a` in a clean worktree, preserving all newer source changes. The repository's primary branch is `master`.

## Integration decisions

- Configuration repairs follow the upstream extraction into `ok_rsi_core/config.py`; the modular controller/runtime remains intact.
- Preserve existing completed-candle duplicate suppression and durable watermark logic together with the new finite input checks.
- Keep episode authorization with its native bounded factory. The new dispatch hook checks current controller pause/stop and episode hold/fault without applying legacy five-second action semantics to candle-scoped episode grants.
- Preserve the episode `actual_base_fee` receipt convention: its quote fees exclude converted base fees, so original quote cost stays intact while actual base inventory decreases. Legacy serialized fills retain their existing converted-fee convention. Both feed the cumulative held ledger.
- Keep persistence-before-start and unfinished-execution reconciliation gates. A failed/local-only check never authorizes account activity.

## Verification

The source changes in the original repair are byte-identified in the original local manifest. Native Cython sources (`*.pyx`, `*.pxd`) are unchanged between the original owner base and current primary branch. The clean integration worktree uses the existing owner-environment compiled extensions; these ignored links are not included in the patch. Eight named local script/controller fixtures needed by existing HL RSI tests were copied into the ignored configuration directories, without credentials.

- Dedicated newer episode suites: 261 passed.
- Original focused integration probe: 112 passed and 29 subtests passed; one outdated factory fixture bypassed orchestrator initialization and was replaced with the initialized persistence boundary.
- The first broad clean-worktree run passed 687 tests; four fixture-loading failures identified the absent ignored local config pack. After providing that named pack, the HL suite passed 11 tests and 3 subtests. The final expanded run is recorded below.
- Native offline OKX tracker → PositionExecutor → PositionHold smoke: PASS, two acknowledged inert submissions, bought 0.4, sold 0.1, remaining 0.3, PnL at 80 = -6, zero active orders and zero network calls.
- flake8, isort, Git diff checks and commit hooks pass for the scoped patch.
- GitHub Actions are intentionally excluded from acceptance at the user's explicit request; no CI success is claimed.

No bot, deployed image, authenticated account or live order was tested or activated. The restart gate and deployment boundary remain documented in `docs/rsi-v5/ok-rsi-lifecycle-repair-runbook.md`.

Final expanded result: **688 passed, 41 subtests passed in 18.96s**, exit 0, no warnings.

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/test_ok_rsi*.py test/test_hl_rsi_controller.py test/hummingbot/strategy_v2/executors/position_executor test/hummingbot/strategy_v2/executors/order_executor test/hummingbot/strategy_v2/executors/test_executor_orchestrator.py test/hummingbot/strategy_v2/executors/test_held_inventory_accounting.py test/hummingbot/strategy/test_strategy_v2_base.py test/hummingbot/strategy_v2/utils/test_market_analysis.py test/hummingbot/connector/test_markets_recorder.py test/hummingbot/connector/exchange/okx test/hummingbot/data_feed/candles_feed/okx_spot_candles test/hummingbot/strategy_v2/migrations/test_ok_rsi_history.py -q --basetemp=/tmp/ok-rsi-publication-verified
```

Audit decision: **ready with noted risk** for repository integration. Authenticated runtime and promotion remain unverified. A new counterexample involving ownership, fee conservation, admission or persistence ordering reverses this decision.
