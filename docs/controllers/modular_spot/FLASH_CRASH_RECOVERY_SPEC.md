# Flash-crash protection: superseded controller-private specification

Updated 2026-09-20. The current rsibot-stack v2 design is:

- [Crash protection and recovery acquisition specification](../../strategy_v2/crash_recovery/SPEC.md)
- [Gated implementation and research ExecPlan](../../strategy_v2/crash_recovery/EXECPLAN.md)

This path remains a compatibility link for the document first published in PR #55. PRs #55, #57 and #59 are now merged; the old task branch is not the implementation destination.

The new design composes versioned neutral components, isolated v2 recipes and explicitly supported native/research adapters. It does not add behavior to `ok_rsi`, `rsi_v5`, existing `rsi_modular` profiles or their shared compatibility exports. `hummingbot.strategy_v2.components` is the canonical neutral library; strategy composition remains outside it.

The earlier intent remains: preserve acquisition capital during stress, require causal recovery evidence, use bounded DCA, retain existing loss/exit policy and evaluate after-cost outcomes on the required cached May-August corpus. Buying the exact future low is not promised.

The [original 2026-09-19 specification](https://github.com/s1korrrr/hummingbot/blob/cdafa2539e8bd6d51f4634e403b51192eb1cb2b7/docs/controllers/modular_spot/FLASH_CRASH_RECOVERY_SPEC.md) is preserved in Git. Its private-module placement, old checkpoint, earlier capability assumptions and M0-M8 directions must not override the current spec or source evidence.

Status: documentation only. No crash-policy implementation, economic run, deployment, state migration or activation is delivered or authorized by this update.
