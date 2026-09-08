# OK RSI lifecycle repair and restart contract

Local remediation of the 2026-09-08 source audit (F1–F18). Owner: Hummingbot. Authority: repository-process-only. This change does not activate a bot or certify account reconciliation, an exchange session, profitability, or live promotion.

## Changed behavior

- PositionExecutor retains every filled entry and exit, including canceled partial orders, and retries only the remaining quantity. Partial entries are eligible for timeout and trailing protection. Shutdown drives the normal close/fallback loop and waits for cancellation before replacement. Untradeable residual quantities are handed to held inventory.
- Recovery OrderExecutor waits for terminal cancellation before chasing again. Its limit floor includes the current bag's remaining entry fees and the connector's estimated exit fee. Market recovery is rejected when profitable-only recovery is required. Actual exchange fees, maker rejection, and unfilled limits remain observable execution outcomes; an estimate is not a guaranteed net return.
- OKX submission/status/cancel transport errors retain client-order ownership. Only an explicit rejection can authorize rejection handling. Canceled/filled status is withheld until the reported cumulative fill quantity is reconciled. A timeout can therefore keep an executor pending; it cannot authorize duplicate submission.
- Held accounting separates active executor orders, closed executor history, remaining inventory and lifetime realized PnL. Base-asset fees reduce owned quantity. Fees from closed inventory are not charged to a later bag. Flat realized history remains in drawdown PnL. Invalid executor performance makes risk reports unavailable rather than substituting zero.
- Entry admission includes pending commitments, shutdown executors and shared-wallet commitments not yet represented by acknowledged orders. Queued creates recheck current reports, operator commands, kill switches, losses and drawdown. Unowned restored OKX orders block creates for their pair.
- OK RSI decisions use closed candles. Future timestamps, missing bars, invalid OHLCV and missing indicator inputs block signals. Bollinger width is calculated explicitly with population standard deviation. HMM smoothing counts each candle once, and confidence belongs to the returned regime. The heuristic mean-reversion score is not a calibrated probability; no new economics were measured here.
- Configuration swaps are atomic. Generated candle defaults are normalized on file reload. Feed topology/history settings require restart. Trailing/entry-timeout settings affect newly created executors; existing executor contracts remain fixed. Implementation hot reload is disabled for OK RSI to preserve runtime risk state.

## Durable state and fail-closed restart

Executor intent is written to the existing `Executors` table before the executor starts. A write failure prevents start. Strategy drawdown peaks, exited-controller IDs, stopped state and operator entry pauses are stored in the existing `Metadata` table under `strategy-risk:<config_file_path>`; no schema migration is needed. Historical completed executor results hydrate the loss-streak gate before proposals.

Automatic recreation of unfinished executors is intentionally unsupported. Recreating them without complete trailing/order state could submit duplicate orders. Startup latches each controller with unfinished persisted execution. Existing execution history without a durable risk record also blocks the OK RSI strategy, as do unreadable/malformed risk state and scope mismatch. A normal configuration reload does not clear these gates.

Run the read-only diagnostic against an explicitly selected database:

```bash
cd /Users/s1kor/dev/trading/rsibot/hummingbot
/Users/s1kor/anaconda3/envs/hummingbot/bin/python scripts/diagnostics/ok_rsi_recovery_status.py /absolute/path/to/bot.sqlite --config-file exact-recorder-config.yml
```

The diagnostic opens SQLite with `mode=ro`, lists unfinished executor and known order identities, and displays the saved risk record. It does not contact OKX, modify the database, clear a gate or prove venue reconciliation. An empty local order list is not evidence that the exchange has no orders.

If it reports a recovery requirement, keep execution stopped. The next external action is an explicitly authorized account reconciliation: query venue order/fill history using both client and exchange order IDs, reconcile remaining quantities and fee assets to the wallet and native ledger, establish the disposition of each unfinished executor, and preserve the account's drawdown peaks and operator pause. Retain the original database and evidence before any approved migration. Do not delete executor rows, mark them complete, reset risk peaks, or choose a fresh database merely to bypass the latch. No automatic clearing tool is supplied because local history alone cannot establish venue truth.

## Validation and deployment boundary

The repair evidence and exact commands are in the completed task plan and local remediation report. Focused regressions use native orders/fills and temporary SQLite; the standalone smoke uses the native OKX tracker and executor event callbacks with all network calls blocked. No credentials or existing account database were read. No bot was started, restarted, deployed or promoted.

These changes include shared engine modules. A controller-file copy cannot deliver them to a deployed bot. Any subsequently authorized runtime validation must use the complete patched Hummingbot package/image, the intended configuration, and an explicitly reconciled account. Region, instrument availability, actual fee tier, reconnect behavior and venue fills remain external validation items. Historical backtests were not rerun and are not comparable evidence for the changed closed-candle policy without execution/feature parity.

## Rollback

Retain the scoped repair commits and do not discard unrelated workspace changes. The publication branch integrates the repair with the newer modular primary branch. No production configuration changed. Roll back only the scoped engine patch while the bot is stopped, retaining both the original and repaired database/evidence. The added Metadata record is compatible with older schema readers, but older engine code lacks these safety gates; rolling back does not make live restart safe.
