# External prediction boundary — review-only hardening

Status: DRAFT; not production approval, trading edge, or authorization to activate a controller.
Base: `65c1645f1ae1e681fc5643535e4b9f779dd6fac2` on this fork's master. The original
ai_livestream.py blob is also identical to rsibot's pinned version.

`external_signal_execution_enabled` defaults to false. This intentionally blocks new entries
for legacy configurations after an eventual rollout. Nothing was deployed or restarted as
part of this patch. Do not enable the flag merely because these tests pass.

The pure validator requires finite numeric timestamps in epoch seconds, bounded age/future
skew, strictly increasing accepted timestamps, exactly three finite probabilities in [0,1]
summing to one, and a positive bounded target_pct. Received predictions are immutable
snapshots. The existing optional target_pct fallback (0.01) is retained and validated.

The controller clears the previous entry signal on invalid/malformed/stale input, checks the
configured topic, rechecks expiry and instrument binding before proposing new entries, abstains
on ambiguous thresholds, and consumes a prediction after an action batch is proposed. The
maximum lifetime is bounded by both event time and local receipt time. No predictor message
or signal means no new action. Executor side must agree with the validated signal.

This does not change Hummingbot executor exit implementations. It does not provide a signed
Risk/Portfolio authorization, persistent replay ledger, producer authentication, durable
execution idempotency, position sizing validation, or an exchange-level order expiry. Replay
watermarks are process-local; a restart does not constitute durable replay protection. Existing
in-flight executors/orders remain owned by Hummingbot. Keep this example controller excluded
from production admission until the canonical authorization and runtime gates are proven.

## Validation evidence

30 pure validator tests passed. Twelve isolated controller-method checks passed with an inert
base/runtime; on the original source, eleven failed and one passed. Python syntax compilation
passed. Local Python was 3.13.5 with installed dependencies, not a complete supported Hummingbot
environment. No full Hummingbot, MQTT, connector, paper, fill, exit, restart, or reconciliation
suite was executed. These are not market-performance or model-behavior evaluations.

Run in the supported full Hummingbot test environment before merge:

```sh
python -m pytest test/hummingbot/strategy_v2/controllers/test_ai_signal_boundary.py \
  test/hummingbot/strategy_v2/controllers/test_ai_livestream_boundary.py -q
```

Then exercise MQTT envelope semantics, actual base-controller compatibility, pending/partial
fills, existing executor exits during signal loss, stale clocks, reconnect and restart, and
independent Risk/Portfolio admission. Use only isolated paper/test services without exchange
credentials. Review default-off migration before any rollout.

The rsibot gitlink is not changed by this companion PR. After review and merge, integrate the
exact approved commit and component lock through the normal root integration gate. Rollback
is a reviewed source revert; never restart or re-enable trading automatically as rollback.
