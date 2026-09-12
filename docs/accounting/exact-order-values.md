# Exact order and fill event receipts

New recorder events retain decimal strings in additive `OrderExact` and
`TradeFillExact` tables. Existing `Order` and `TradeFill` six-decimal integer
columns remain unchanged for compatibility. Both representations commit or roll
back within the same recorder transaction. Repeated event identities do not
create another receipt or order-status row. Conflicting exact quantity/price or
fill side, pair, or fee payload raises an error instead of replacing the receipt.

`hummingbot.model.exact_order_values.read_exact_values(session, order_or_fill)`
returns Decimal amount and price, an optional Decimal quote fee, and a precision
label. `event_exact` means values retained from the recorder event; it does not
mean an independent exchange reconciliation occurred. `legacy_6dp` explicitly
identifies rows without either a native exact or companion receipt. A zero legacy quote fee is treated as
unavailable because historical conversion failures also stored zero. An exact
receipt distinguishes known zero from unavailable conversion through a nullable
quote-fee column. Decimal strings avoid binary floating point and scaled-integer
overflow.

The native recorder joins these receipts when reading trades. Existing native
`TradeFill.exact_*` event receipts remain authoritative and are corroborated
against companion receipts when both are present. Missing companion rows never
downgrade retained native exact values; disagreement blocks the read. Ongoing
held inventory reconciliation and startup reconstruction retain their strict
native-exact and ledger corroboration; compatibility columns are not rewritten. An exact event with unknown
quote-fee conversion blocks reconciliation instead of certifying zero fees.
Other readers of compatibility columns continue to receive six-decimal values
until they opt into the helper. Legacy results retain an explicit precision label.

Startup metadata creation adds missing tables; there is no version bump, file
swap, or rescaling migration. The read helper also supports old databases that
lack these tables, without creating them. Historical lost digits are not
invented or backfilled. Replaying a historical event whose legacy row already
exists leaves it precision-limited; it does not certify legacy data as exact.

Rollback: revert the code change. Existing rows and tables are compatible with
old readers; leave the additive tables in place. No table deletion is required.
Deploying code or restarting a recorder is separate from this local change.

Validation uses temporary/in-memory SQLite and event callbacks with a local
connector fixture; no exchange, account, service, or live database access:

```sh
python -m pytest test/hummingbot/model/test_exact_order_values.py \
  test/hummingbot/connector/test_markets_recorder.py \
  test/hummingbot/model/test_trade_fill.py -q
```
