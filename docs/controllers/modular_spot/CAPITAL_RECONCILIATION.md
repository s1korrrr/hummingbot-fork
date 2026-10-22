# Native candidate funding reconciliation

The funded candidate remains one controller, pair and recorder, with an explicit
allocation no greater than 50 USDC. Native orders, fills, fees and PositionHold
remain accounting truth. This does not reserve money from other wallet owners.

`NativeFundingStore.reconcile()` is the callable reconciliation path. Native
admission calls it before calculating the next acquisition, so successful
cancellation recovery needs no metadata edits or operator override. It returns
`released_intents`, `pending_quote`, `available_quote` and
`unused_intent_release`. It makes no exchange request or order action.

The connector and executor must first provide the evidence:

1. OKX GET order status reports terminal canceled/filled state, matching client
   ID, exchange ID, instrument and side. Its cumulative fill quantity must equal
   the native tracked fills; missing fills are fetched and passed through the
   existing native tracker. Unavailable or incomplete cumulative fills cannot
   produce release evidence.
2. PositionExecutor saves `entry_reconciliation` in its existing custom-info
   record. The receipt includes source/schema, order and exchange IDs, pair,
   side, submitted amount, final filled amount, terminal state and exact trade IDs.
   The single-attempt entry cannot replace that submission. A native retry entry
   can release only after the executor is inactive and a complete terminal set
   covers every native BUY snapshot, including canceled prior attempts. Individual
   attempts must agree with their own recorder fills; aggregate fills must agree
   with the executor's native entry receipt.
3. Funding validates the immutable admitted executor configuration, native gross
   entry receipt, exact recorder fills and fees, native order identity and any
   retained tracker state. It writes an immutable release receipt alongside its
   existing intent/fill hashes using the existing transactional compare-and-swap.
   Only the unused intent is released; inventory cost, cash consumed and fees
   remain charged through native accounting.

An explicit OKX submission rejection produces a separate zero-fill receipt when
there is no exchange order ID or fill. A timeout, lost order, exception message,
local canceled/failed status or elapsed time is never release authority. Paper
receipts have a distinct source and cannot qualify a live owner.

Funded modular wrappers opt into the native executor field
`require_terminal_entry_reconciliation=True`. This preserves OK RSI's
`single_attempt` and RSI V5's `retry` policy, signals, sizes, prices and barriers.
The sole intentional factory divergence from the corresponding unfunded controller
is stronger terminal evidence before shutdown completes. Default false preserves
existing owners. During funded shutdown, connector GET reconciliation runs as a
bounded native task; protective exits keep running while it is pending. Native
held-inventory handoff or final termination waits for complete proof. Paper shutdown
settles/cancels that task explicitly instead of leaving an orphaned coroutine.

Repeated reconciliation is idempotent. A restored recorder can reuse its durable
native executor receipt after all the same checks. Missing or changed intent,
release or fill records, missing executors, unavailable native receipts and an
unfinished executor that was not restored all stop new admissions. Do not delete
these records to clear a hold. Ambiguous submission/crash cases require native
owner reconciliation; this feature does not invent a recovery from absent venue
truth or activate an unresolved executor.

A venue-final cumulative fill is the boundary that makes reuse of unused capital
safe. The code does not claim that an arbitrary delay proves finality. A fill
that contradicts a previously accepted final receipt causes subsequent admissions
to fail closed. Native protective exits continue independently. No software can
retroactively prevent an already admitted order if the venue later contradicts
its own final cumulative result; operator alerts and account reconciliation remain
necessary operational acceptance conditions.

Validation uses the native interpreter and isolated native paper connector:

```bash
/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest \
  test/hummingbot/strategy_v2/paper/test_paper_funding.py \
  test/modular_spot/test_native_funding.py \
  test/hummingbot/connector/exchange/okx/test_okx_order_uncertainty.py \
  test/hummingbot/strategy_v2/executors/position_executor -q --tb=short
```

Tests cover zero/partial fills, local cancel without authoritative proof,
subsequent admission, explicit rejection versus uncertain submission, restart,
idempotence, mismatched receipt scope/IDs/quantity/trades, missing audit/native
records, contradictory late fills and real native protective SELL execution.
They do not query a normal trading account, send live orders, deploy a service,
or grant economic/promotion acceptance. Candidate deployment must bind the updated
engine package/image, including `terminal_reconciliation.py`; copying controllers
alone is insufficient. Existing live owners and configurations remain untouched.

Rollback is an isolated candidate source/image rollback before activation. If a
candidate has already persisted new release rows, preserve its recorder and native
receipts and stop new admissions for explicit reconciliation; older code will
reject the unfamiliar release audit records rather than silently reuse funds.

Validation receipt (2026-09-20): the full prior native regression command, with
OKX order-uncertainty coverage added, passed **1,151 tests and 65 subtests, zero
skips** in 100.91 seconds. Scoped Flake8 and `git diff --check` passed. The repository pre-commit hooks, including isort, passed. The exact full command and log were retained for the root handoff.


RSI V5 extension validation additionally exercises its unchanged native retry
policy: first canceled attempt, native replacement with a distinct order ID,
optional partial fill, native shutdown, all-attempt terminal reconciliation,
recorder restart and capacity release. Removing one attempt from the terminal set
holds all release. This is native paper execution evidence, not a synthetic policy
object or a change of RSI V5 to single-attempt semantics.

The RSI V5 extension's full native regression passed **1,152 tests and 67
subtests, zero skips** in 108.58 seconds. An additional real native paper test
then held the terminal GET pending while RSI V5's protective MARKET SELL flattened
the acquired position; the executor waited for proof before terminating and
releasing unused funding. That additional focused test passed independently.
