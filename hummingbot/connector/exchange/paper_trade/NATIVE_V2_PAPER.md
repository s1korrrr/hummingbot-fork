# Isolated native V2 paper execution

`NativePaperTradeExchange` is an opt-in `ExchangeBase` connector. It creates no exchange client, transport, credentials, or network tasks. The existing Cython paper connector and ordinary connector factory are unchanged. Native `ClientOrderTracker`, `InFlightOrder`, `TradingRule`, `BudgetChecker`, fee classes, executor events, and `MarketsRecorder` remain the order and accounting interfaces.

Use `create_native_paper_trade_market(trading_rules, fee_schema, initial_balances, ...)` explicitly. `is_native_paper_connector` requires the exact connector type and `okx_paper_trade` name; a name string or subclass is insufficient. Native modular controller configuration must separately select `execution_mode=paper`, `connector_name=okx_paper_trade`, and `candles_connector=okx`. This is not a deployment or activation command.

The connector accepts a monotonic simulation clock, fresh ordered book snapshots through `set_order_book`, and separately observed public prints through `process_trade`. `drain` waits for submitted native order/cancellation operations. MARKET and crossing LIMIT orders consume each injected depth snapshot once; MARKET is IOC, cancelling any unfilled residual. A repeated snapshot cannot replenish consumed liquidity. Newer snapshots explicitly assert newly observed available liquidity. Stale/future inputs and empty market depth reject execution.

Resting LIMIT/LIMIT_MAKER fills share a print's bounded quantity in price/time order. `trade_participation` defaults to 0.1 and must be explicit in a fixture that assumes full participation. This fraction does not establish real queue position or venue fill probability. Candles never create fills. Slippage comes only from explicitly supplied depth levels; the model does not infer market impact or replenishment between observations.

Native fee schemas support percentage maker/taker fees with quote-cost BUY or base-deducted BUY, and quote-deducted SELL. Fill events contain the actual charged `TokenAmount`, not percentage estimates. Third-token fees, fixed fees, rebates, leverage, and perpetual accounting are rejected. Remaining reservations include the greater applicable maker/taker fee for LIMIT orders. Native quantity, notional and balance checks remain enforced.

`NativePaperSession` in `hummingbot.strategy_v2.paper_session` binds complete native modular controllers to the real `ExecutorOrchestrator`, real executors, injected market data and a disposable SQLite recorder. Its scheduler callback steps the unchanged executor classes explicitly instead of starting wall-clock control loops. An isolated native RateOracle receives only injected prices and is restored on exit. An existing recorder or running oracle prevents session creation.

The session admits contiguous, aligned, fully closed candles with valid OHLC/volume and explicit availability time. This closed-candle replay contract does not prove equivalence to RSI V5's provisional candle timing. `step(timestamp, books=..., trades=..., candles=...)` returns actual actions, native reports, connector state, and readiness reasons.

Recorder state preserves balances, native fills/orders and input sequence watermarks. Restore discards cached liquidity and requires a newer fresh book. A separate session marker binds source/runtime bytes, actual native extensions, Python version, full controller configs, initial balances and connector fee/rule/input assumptions. Other databases or changed contracts cannot be silently adopted. Terminal status reconciliation uses the native paper tracker; unavailable identity remains a hold.

Orderly `close()` cancels remaining paper orders, completes native hold transitions, persists recorder-backed inventory and updates an already restored native recovery checkpoint while retaining existing trail fields. It does not run another policy decision or invent a trailing peak. Abrupt failure between recorder and checkpoint remains an explicit recovery HOLD on restart. Tests cover both outcomes. This does not authorize adoption of live owners or prove planned live resume.

Validation is synthetic execution correctness evidence. It is not an economic result, public-feed end-to-end validation, production deployment binding, or paper/live promotion. Public candles, observed book/trade ingestion, source health, risk limits and deployment need their own explicit runtime evidence.

Each session step validates the complete book/trade/candle batch before changing
its clock, input watermarks, liquidity, or native fills. A malformed batch is
rejected atomically. An fsynced `.paper-step.json` journal is written before an
accepted batch has effects and removed only on successful completion. A runtime
failure preserves actual native fills, balances, tracked orders and executor
snapshots; it never rolls back a fill. The journal retains the input digest and
failure evidence, and even a failed final receipt write leaves the earlier
interruption journal. Such a session rejects further steps and placement routes,
and stays held across close/reopen. Closing it cannot certify a clean recovery
checkpoint. Explicit offline reconciliation is required; automatic recovery of a
failed step is deliberately unsupported. Filesystem or database failures can
prevent complete persistence, but cannot grant automatic resume while the
pre-effect journal remains.
