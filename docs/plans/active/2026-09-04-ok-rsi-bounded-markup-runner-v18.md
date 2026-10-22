# OK RSI bounded markup runner v18

## Objective and completion bar

Test whether the existing structure-held runner can improve sell placement
without suppressing later buys when it is allowed only before the final
distribution threshold and is released as soon as hourly markup ends. Complete
the slice when the native reducer, runtime adapter, durable receipts, and
default-off compatibility are tested, then hand the exact source commit to the
Quants Lab May-August replay.

## Frozen hypothesis

V8 proved that markup release preserves C0 buy identity, but its unrestricted
runner retained inventory after a fully extended BNB move and surrendered the
gain. V15 showed the opposite opportunity: the May 30 BNB runner improved its
exit from 704.7 to 716.1, while the May 6 runner entered after the third
distribution threshold and fell from 657.9 to 646.8. A runner is therefore
eligible only when the live distribution extension remains below the existing
last distribution threshold. This reuses a configured state boundary and adds
no fitted numeric threshold.

The treatment is invalidated if it changes any V15 buy root, lowers filled buy
count, retains less than 98 percent of V15 buy quote, creates an underwater
sell, exceeds the V15 deployment cap by more than ordinary mark quantization,
or fails to improve aggregate PnL by at least 5 USDC with non-regressing
drawdown. May-August is the only economic evaluation window; no parameter is
tuned after seeing its result.

## Scope and ownership

- Owning repository: native `hummingbot/` controller worktree.
- Code scope: `controllers/directional_trading/ok_rsi_core/episode.py`, public
  config/runtime wiring, focused OK RSI tests, and plan pointers.
- Research scope after the native commit: Quants Lab owner replay and report.
- Modes: shared `buy_sell` and `sell_only` reducer behavior; the latter must
  remain buy-inert.

## Authority and forbidden surfaces

Authority is `research-only`. Do not activate paper, canary, or live bots; do
not call an exchange, place orders, deploy, push, open a pull request, or change
capital configuration. The option remains disabled by default.

## Design

1. Add an opt-in bounded markup runner flag that requires the existing
   structure-held and markup-release runner flags.
2. On a trailing breach with recovered hourly structure and clear risk, retain
   the configured final runner only while the causal distribution extension is
   below `distribution_atr_levels[-1]`.
3. At or above that final threshold, take the ordinary full trailing exit.
4. A retained runner still exits on markup loss, hourly structure loss, or risk
   override through the existing protected sell path.
5. Emit a distinct policy receipt and expose the eligibility boundary and
   active state in status/checkpoints.
6. Preserve exact behavior and checkpoint identity while disabled.

## Validation

- Focused pure reducer tests for below-boundary retention, boundary/full exit,
  markup loss, risk override, restart, and `sell_only` buy-inert behavior.
- Runtime/config/factory tests for dependency validation, receipt propagation,
  status, and disabled identity.
- Ruff, focused native OK RSI suites, `git diff --check`, and a clean staged
  diff containing only intentional paths.
- Exact-source Quants Lab bounded replay followed by continuous May-August
  BTC/ETH/BNB replay at 15 bps per side under observed-touch full-fill semantics.

## Rollback

Keep the feature flag false or revert the isolated V18 commits. Existing
checkpoints and V15 behavior retain their identities when the flag is false.

## Progress

- [x] Diagnose V7/V8/V15 and freeze the causal boundary before implementation.
- [x] Add failing native behavior/config/receipt tests.
- [x] Implement the disabled-by-default native treatment.
- [x] Run native validation and commit the exact source.
- [ ] Bind and replay the treatment in Quants Lab.
- [ ] Freeze the development verdict and detailed Plotly report.

## Native evidence

- RED: the focused selection failed in seven places because neither the pure
  config nor public runtime knew the bounded treatment.
- GREEN: `279 passed, 19 subtests passed` across the OK RSI reducer, context,
  runtime, execution, factory, entry-pricing, controller, and wallet suites.
- Static checks: Ruff and `git diff --check` passed on every touched Python
  path.
