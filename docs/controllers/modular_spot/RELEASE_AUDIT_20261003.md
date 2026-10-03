# Signed native fee compatibility — 3 October 2026

The funding projection and terminal-entry reconciliation now accept finite
signed native fee amounts in their actual base or quote currency. A negative
native cost is a maker rebate. Rejecting it made valid recorded inventory
unavailable in the V2 admission reader, V3 portfolio/dust reader and stopped
recovery proof. The exact native fill adapter and PositionHold already support
these fee signs. This correction changes three decimal parser calls; it does
not add another cost ledger or release policy.

The guards for exact native receipts, matching fee valuation, zero percentage
fee, supported base/quote currency, owner/order/fill identity, duplicate fills,
finite numbers, no sales of unowned inventory, nonnegative remaining acquisition
cost and the configured maximum acquisition fee remain. A negative fee cannot
make a third-token or contradictory receipt valid. Legacy explicit
`actual_base_fee` records retain their existing separate schema; this repair
uses native exact TradeFill and order-fill receipts.

## Source identity

The V2 modular closure deliberately changes from
`17cba31a66162942cf9a59c10ef6e68d82028e6e614b341f2521a5a6d13951c8` to
`e86f4e08f46ccf49a4fa80fd5647b6ce45db2ca835034cab8b05bd6cf4d400cf`.
Both manifests contain the same 67 paths; only
`controllers/generic/modular_spot_core/native_funding.py` changes. The frozen
isolation regression records this qualified change and separately excludes the
V3-only terminal-dust helper and crash-policy modules. Moving that helper out
of modular_spot_core first preserved the former digest; no unrelated source
was silently added to V2's contract.

Existing research receipts, native checkpoints and deployed bindings keep their
actual historical identities. They are not updated by this source change.
Any V2 or V3 deployment needs a separately approved source/funding/held-state
migration qualified on a current native clone and fresh venue evidence. The
existing runtime source-upgrade mechanism is not evidence that the complete
restart is qualified. Do not replace a live binding or erase a checkpoint to
make the image start. Test images are not deployment candidates.

## Executable evidence

Four V3 terminal snapshot regressions cover BUY/SELL times base/quote rebates;
all failed on the pre-fix image at the old nonnegative valued-fee guard and pass
with the correction. Shared funding tests independently derive inventory, cash
and remaining cost. Their signed terminal-entry release exercises actual native
order/fill receipt validation and retains the positive fee ceiling test. All
14 tests in that module pass with the source correction; the pre-fix image
fails six newly added cases. Remaining full-PR verification and exact revision
identities are in the fleet recovery review report.

Application tests run only in disposable network-disabled read-only containers,
with synthetic native records and no production credentials/mounts. No strategy
parameters, capital limits, live state or order behavior were changed.

The source contract follows [OKX API v5](https://www.okx.com/docs-v5/en/) actual
fee currency and sign, as normalized by this fork's OKX connector, and the native
`ExecutorOrchestrator._trade_fill_order_record` / `PositionHold` accounting.
