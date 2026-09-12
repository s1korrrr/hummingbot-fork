# OK RSI single-attempt entry decisions

Normal OK RSI BUY position proposals use `entry_order_policy=single_attempt`.
This policy requires the corresponding PositionExecutor implementation. A decision
is durably consumed before its action enters the executor queue. Executor failure,
cancellation, expiry and a process restart do not authorize another attempt.

On first use, the controller waits for a healthy, completed, owner-evaluated candle
that clears the BUY candidate and any pending confirmation. Only a later qualified
BUY candle can authorize one attempt. On restart, existing consumed state remains;
a queued authorization from the previous controller instance is never resumed.
The existing unfinished-executor and unowned-order reconciliation blocks remain.

`signal=0` on a repeated candle is merely an emission pulse reset. It does not
clear a setup. Rearming requires an advancing completed candle with an explicitly
false `raw_buy_candidate`, no active confirmation, no BUY signal, healthy data and
`condition_ok=true`. A later confirmation BUY may qualify without another raw
reversal. DCA additions still require a fresh controller decision and existing
exposure, balance, spacing and cooldown checks.

Admission stores a versioned record in the native recorder's SQLite Metadata table,
scoped by script config, controller, connector, pair and interval. State contains
the evaluated/consumed candle, armed state, authorized executor ID, configuration
fingerprint and one-shot dispatch state. Compare-and-swap rejects stale writers.
Missing recorder, invalid state or failed persistence blocks BUY admission and
reports an error in `entry_decision_admission`. There is no in-memory production
fallback. Local tests must explicitly inject an entry-decision store.

SELL management remains independent of BUY admission storage. Episode runtime
reconciliation, stop actions and SELL exits continue through their existing gates.
Only new episode BUY actions are filtered and rejected at queued validation until
that separate authorization path supports this contract. There is no activation
or restart in this change.

Dispatch consumes the exact authorized executor identity and configuration once,
after existing controller risk checks. A crash between durable dispatch and actual
executor startup may miss participation. It cannot silently recreate the attempt.
The executor intent is separately persisted before executor startup by the existing
orchestrator. These are deliberately fail-closed boundaries, not exactly-once
exchange submission guarantees.

Validation: `python -m pytest test/test_ok_rsi_entry_decisions.py -q` in the native
Hummingbot environment. Tests use local SQLite and mocked market inputs; they do
not connect to an exchange or grant deployment/trading authority.
