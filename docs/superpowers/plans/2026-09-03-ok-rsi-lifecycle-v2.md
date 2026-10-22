# OK RSI Lifecycle V2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the approved opt-in lifecycle-v2 native controller behavior and preserve v1 and legacy defaults.

**Architecture:** Extend the pure `ok_rsi_core` reducer and completed-candle context, map explicit public configuration through `EpisodeRuntime`, and transport durable request intent through the bounded native order executor. `ok_rsi.py` remains a validation and dispatch adapter.

**Tech Stack:** Python, Pydantic controller configuration, Hummingbot Strategy V2 executors, pytest.

**Spec:** `docs/superpowers/specs/2026-09-03-ok-rsi-lifecycle-v2-design.md`

## Global Constraints

- Local research authority only; no paper, live, deployment, exchange, push, or PR action.
- Preserve legacy controller and lifecycle-v1 defaults.
- Keep 1x spot aggregate accounting fill-driven and profit-only.
- Add no production behavior without first observing the focused regression test fail.
- Preserve unrelated repository and workspace changes.

---

### Task 1: Configuration and causal context

**Files:**
- Modify: `controllers/directional_trading/ok_rsi_core/config.py`
- Modify: `controllers/directional_trading/ok_rsi_core/context.py`
- Modify: `controllers/directional_trading/ok_rsi_core/runtime.py`
- Test: `test/test_ok_rsi_episode_context.py`
- Test: `test/test_ok_rsi_episode_runtime.py`

**Interfaces:**
- Produces: public lifecycle-v2 fields; `hourly_recovered: bool` in `MarketSnapshot`; complete `EpisodeConfig` mapping.

- [x] Write focused configuration/context/runtime tests for default compatibility, finite positive budget validation, completed two-hour recovery, and policy mapping.
- [x] Run the tests and verify failures identify missing fields and context.
- [x] Implement the smallest validated fields, context calculation, and mapping.
- [x] Run the focused tests to green.

### Task 2: Causal entry controls

**Files:**
- Modify: `controllers/directional_trading/ok_rsi_core/episode.py`
- Test: `test/test_ok_rsi_episode.py`

**Interfaces:**
- Consumes: `EpisodeConfig` C1-C3 flags and `MarketSnapshot.hourly_recovered`.
- Produces: sequential scale-ins, fresh-signal gate, configurable stabilization, hostile risk latch, and re-armable setup expiry.

- [x] Add one failing reducer test for each C1-C3 behavior and a compatibility test for v1 defaults.
- [x] Run each focused test and verify its intended RED failure.
- [x] Implement the minimum pure reducer changes.
- [x] Run the episode suite to green.

### Task 3: Recovery profit exit and intent transport

**Files:**
- Modify: `controllers/directional_trading/ok_rsi_core/episode.py`
- Modify: `controllers/directional_trading/ok_rsi_core/execution.py`
- Modify: `controllers/directional_trading/ok_rsi_core/runtime.py`
- Modify: `controllers/directional_trading/ok_rsi.py`
- Test: `test/test_ok_rsi_episode.py`
- Test: `test/test_ok_rsi_episode_runtime.py`
- Test: `test/test_ok_rsi_episode_execution.py`
- Test: `test/test_ok_rsi_episode_factory.py`

**Interfaces:**
- Produces: profit-floor helper, recovery trail, typed request intent, executor config `episode_intent`, and receipt `intent`.

- [x] Add failing tests for profitable and below-floor recovery, assigned sell-only recovery, buy/sell intent, factory tampering, and receipt metadata.
- [x] Run the focused tests and verify the expected RED signals.
- [x] Implement recovery arming and intent transport without bypassing the profit guard.
- [x] Run all focused lifecycle tests to green.

### Task 4: Native verification and source freeze

**Files:**
- Modify: `PLAN.md`
- Modify: `TODO.md`
- Modify: `docs/superpowers/plans/2026-09-03-ok-rsi-lifecycle-v2.md`

**Interfaces:**
- Produces: exact source SHA and validation evidence for Quants Lab replay.

- [x] Run Ruff or repository formatting checks for changed files.
- [x] Run all OK RSI episode/context/runtime/execution/factory tests.
- [x] Run `git diff --check`, inspect the scoped diff and status, and record exact results.
- [ ] Record the native source commit or dirty source receipt used by research without pushing.

## Verification

- `conda run -n hummingbot pytest -q test/test_ok_rsi_episode.py test/test_ok_rsi_episode_context.py test/test_ok_rsi_episode_runtime.py test/test_ok_rsi_episode_execution.py test/test_ok_rsi_episode_factory.py`
- `conda run -n hummingbot ruff check controllers/directional_trading/ok_rsi.py controllers/directional_trading/ok_rsi_core test/test_ok_rsi_episode.py test/test_ok_rsi_episode_context.py test/test_ok_rsi_episode_runtime.py test/test_ok_rsi_episode_execution.py test/test_ok_rsi_episode_factory.py`
- `git diff --check`

## Decision Log

- 2026-09-03: User approved the combined lifecycle-v2 and Plotly-report design.
- 2026-09-03: Preserve profit-only exits for the first experiment; test a risk exit only if bounded entry behavior cannot meet the drawdown gate.
- 2026-09-03: Use explicit opt-in fields so legacy and lifecycle-v1 behavior remain stable.

## Progress Log

- 2026-09-03: Approved design recorded; implementation not started.
- 2026-09-03: RED-GREEN cycles completed for C1-C4, restart-safe state, and native intent transport.
- 2026-09-03: Validation passed: 177 tests and 19 subtests, Ruff, and `git diff --check`.
- 2026-09-03: Next: commit the native source and bind its SHA into Quants Lab research.

## Rollback / Recovery

- Revert only the lifecycle-v2 branch or disable its opt-in fields.
- Preserve every unrelated root and nested worktree change.
