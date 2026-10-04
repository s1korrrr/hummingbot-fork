# Signed native fee compatibility — 3 October 2026

The funding projection and terminal-entry reconciliation now accept finite
signed native fee amounts in their actual base or quote currency. A negative
native fee is a maker rebate. Rejecting it made valid recorded inventory
unavailable in the V2 admission reader, V3 portfolio/dust reader and stopped
recovery proof. The exact native fill adapter and PositionHold already support
these fee signs. This correction changes three decimal parser calls and verifies that the
recorded quote valuation equals the exact actual-currency fees: quote fees
plus base fees multiplied by the exact fill price. This is the native recorder
conversion contract, not another cost ledger or release policy.

The guards for exact native receipts and quote-fee encoding, zero percentage
fee, supported base/quote currency, owner/order/fill identity, duplicate fills,
finite numbers, no sales of unowned inventory, nonnegative remaining acquisition
cost and the configured maximum acquisition fee remain. Both terminal-entry
release and the general projection compare actual-currency fee valuation with
the native recorded value. A matching rounded/exact encoding alone does not
prove the value belongs to the actual fee. A negative fee cannot
make a third-token or contradictory receipt valid. Legacy explicit
`actual_base_fee` records retain their existing separate schema; this repair
uses native exact TradeFill and order-fill receipts.

## Source identity

The V2 modular closure deliberately changes from
`17cba31a66162942cf9a59c10ef6e68d82028e6e614b341f2521a5a6d13951c8` to
`9803842ea19120739feeb0c367c6999f5d19f2a1514cde41653dad5d66fc7885`.
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
22 tests in that module pass with the source correction. The first pre-fix
image fails six signed-fee cases; the intermediate signed-only candidate fails
six contradictory-valuation cases. Two mixed base/quote fee cases preserve
independently derived cost and inventory. Remaining full-PR verification and exact revision
identities are in the fleet recovery review report.

Application tests run only in disposable network-disabled read-only containers,
with synthetic native records and no production credentials/mounts. No live
configuration, state or orders were changed. No new sizing or risk policy was
introduced.

The source contract follows [OKX API v5](https://www.okx.com/docs-v5/en/) actual
fee currency and sign, as normalized by this fork's OKX connector, and the native
`ExecutorOrchestrator._trade_fill_order_record` / `PositionHold` accounting.

## Native candle ownership follow-up — 3 October 2026

The frozen 67-file V2 modular source closure has a later, separately identified
change: its digest advances from
`9803842ea19120739feeb0c367c6999f5d19f2a1514cde41653dad5d66fc7885` to
`05dd2199882fcf1662dde6b774e7613e437c292586ffdf34fe0b28f798db011a`. A
comparison of the exact source-file list shows that only
`modular_spot_core/native_composition.py` changed inside the closure. The
additional OK RSI config validation change in `directional_trading/ok_rsi_core/config.py`
is outside this 67-file closure and remains governed by its own controller
source identity.

The composition now reports that it does not own the direct OK RSI candle
service route; its native candle provider remains the source for shared
composition data. The 67 paths and their inclusion rules are unchanged. This
updates the current closure golden only; prior manifests, release identities,
and historical bindings retain their recorded digests. The native image's
source identity remains a separate deployment and recovery gate.
