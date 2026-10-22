# t17b — Refresh working-tree review package after t16

Status: **DONE_WITH_CONCERNS**

Output: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff`

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
HEAD: `4a4372bcde4990355421a42df3b8ce6107299abb` (unchanged)
Git: no commit, no push, no PR. USER_REQUESTED_GIT=no.
Hummingbot checkout: not touched (`feat/andrzej_telegram_ssl_log_noise`, still dirty).

Package is working tree vs `4a4372bc` (index + unstaged combined). Commands: `git status --short`; `git diff --stat 4a4372bc`; `git diff -U10 4a4372bc`. Size 205021 bytes, 4306 lines.

## Diffstat (WT vs 4a4372bc)

```
 controllers/directional_trading/hl_rsi.py          | 581 +++++++++++++++------
 controllers/directional_trading/ok_rsi.py          |   4 +-
 .../ok_rsi_core/down_only_entry_execution.py       | 529 ++++++++++++++-----
 hummingbot/connector/exchange/okx/okx_exchange.py  |  16 +
 hummingbot/core/data_type/in_flight_order.py       |  25 +-
 .../position_executor/position_executor.py         |  46 +-
 .../exchange/okx/test_okx_order_uncertainty.py     | 124 +++++
 .../core/data_type/test_in_flight_order.py         | 227 ++++++++
 .../position_executor/test_position_executor.py    |  66 ++-
 .../test_trailing_no_loss_floor.py                 | 136 ++++-
 test/test_hl_rsi_entry_control.py                  | 483 +++++++++++++++++
 test/test_ok_rsi_admission_safety.py               | 164 ++++++
 test/test_ok_rsi_down_only_entry_execution.py      | 470 ++++++++++++++++-
 test/test_ok_rsi_down_only_entry_factory.py        |  38 +-
 14 files changed, 2604 insertions(+), 305 deletions(-)
```

Delta vs t14b package (2485/305): **+119 insertions**, 0 extra files. Grew in `hl_rsi.py` (547→581), `test_hl_rsi_entry_control.py` (440→483), `test_ok_rsi_admission_safety.py` (122→164). Matches t16 covering-sum files. Package includes `test_new_active_hold_before_stored_prior_hold_refuses_new_buy`, `test_new_shutting_down_hold_before_stored_prior_hold_refuses_new_buy`, and `test_new_hold_without_prior_claimant_refuses_leftover_sized_bag`.

Status short: 9 `MM`, 4 staged-only `M `, 1 unstaged-only ` M` (`test_in_flight_order.py`), 1 `AM` (`test_hl_rsi_entry_control.py`). Same mixed-index shape as t14b.

## Concerns

1. Mixed index vs worktree. Staged snapshot is still 13 files, 1283 insertions / 291 deletions. Unstaged remainder is 10 files, 1362 / 55 (was 1243 / 55 at t14b). `git commit` of the index would not match this review package. `test_in_flight_order.py` is not in the index.
2. Reviewer must read WT contents, not `--cached`. Same mixed-`MM` note as t8b/t11b/t14b; still uncommitted by instruction.
