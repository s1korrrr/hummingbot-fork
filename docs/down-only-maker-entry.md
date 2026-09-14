# OK RSI bound-driven maker entry

The five Main spot controllers use `down_only_maker_v1` with a dedicated
`entry_activation_bound_pct: '0.0005'` (0.05%). Adaptive entry-spacing settings
do not change this bound. SUI sell-only is not an acquisition owner.

Validity uses the current OKX best-bid/best-ask midpoint, compared with the
original entry price plus or minus the frozen percentage. Both endpoints are
inclusive. For 722.7, the band is 722.33865 through 723.06135. A midpoint of
723.0 remains valid after 30 seconds; 723.1 or 722.3 ends acquisition.

There is no 30-second resting-order expiry under this policy. The independent
300-second maximum intent age remains. Exiting the band or reaching that age
cancels only the unfilled remainder; acquired inventory remains managed.

A bound permits waiting, not a guaranteed fill. A resting buy fills only when
exchange matching reaches it with sufficient sell volume. A touched resting
order is left for exchange fill reconciliation rather than chased away.
Downward repricing requires the existing movement threshold and cooldown,
confirmed cancellation and fill reconciliation, and a fresh valid book before
the replacement is submitted. Upward repricing and taker conversion are absent.
Any partial or late fill disables further replacement. Fully filled entries
leave acquisition management and retain normal position management.

Structured decision logs include `activation_bound_pct`, `activation_reference`,
`mid_price`, lower/upper prices, reason, age, fills, and reprice count. Native
single-attempt reconciliation remains responsible for terminal order truth.

Rollback requires a stopped, reconciled owner and the matching prior image and
config backup. Do not switch a live executor's semantics in place or restart
shared services to deploy this Main-only change.
