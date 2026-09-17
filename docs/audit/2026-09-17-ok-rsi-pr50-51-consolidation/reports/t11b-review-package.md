# t11b — Refresh working-tree review package after t10

Status: **DONE_WITH_CONCERNS**

Output: `/Users/s1kor/dev/trading/rsibot/.cursor/orchestra/runs/20260917T063417Z/reviews/review-4a4372bc-worktree.diff`

Worktree: `/Users/s1kor/dev/trading/rsibot/.worktrees/hummingbot-ok-rsi-audit-consolidation-20260917`
Branch: `feat/andrzej_ok_rsi_audit_consolidation`
HEAD: `4a4372bcde4990355421a42df3b8ce6107299abb` (unchanged)
Git: no commit, no push, no PR. USER_REQUESTED_GIT=no.
Hummingbot checkout: not touched (`feat/andrzej_telegram_ssl_log_noise`, still dirty).

Package is working tree vs `4a4372bc` (index + unstaged combined). Commands: `git status --short`; `git diff --stat 4a4372bc`; `git diff -U10 4a4372bc`. Size 194296 bytes, 4078 lines.

## Diffstat (WT vs 4a4372bc)

```
 controllers/directional_trading/hl_rsi.py          | 525 ++++++++++++++------
 controllers/directional_trading/ok_rsi.py          |   4 +-
 .../ok_rsi_core/down_only_entry_execution.py       | 529 ++++++++++++++++-----
 hummingbot/connector/exchange/okx/okx_exchange.py  |  16 +
 hummingbot/core/data_type/in_flight_order.py       |  25 +-
 .../position_executor/position_executor.py         |  46 +-
 .../exchange/okx/test_okx_order_uncertainty.py     | 124 +++++
 .../core/data_type/test_in_flight_order.py         | 227 +++++++++
 .../position_executor/test_position_executor.py    |  66 ++-
 .../test_trailing_no_loss_floor.py                 | 136 +++++-
 test/test_hl_rsi_entry_control.py                  | 393 +++++++++++++++
 test/test_ok_rsi_admission_safety.py               |  83 ++++
 test/test_ok_rsi_down_only_entry_execution.py      | 470 +++++++++++++++++-
 test/test_ok_rsi_down_only_entry_factory.py        |  38 +-
 14 files changed, 2377 insertions(+), 305 deletions(-)
```

Delta vs t8b package (2315/305): **+62 insertions**, 0 extra files. Grew in `hl_rsi.py` (502→525), `test_hl_rsi_entry_control.py` (369→393), `test_ok_rsi_admission_safety.py` (68→83). Matches t10 hold-flag files.

Status short: 9 `MM`, 4 staged-only `M `, 1 unstaged-only ` M` (`test_in_flight_order.py`), 1 `AM` (`test_hl_rsi_entry_control.py`).

## Concerns

1. Mixed index vs worktree. Staged snapshot is 13 files, 1283 insertions / 291 deletions. Unstaged remainder is 10 files, 1133 / 53. `git commit` of the index would not match this review package. `test_in_flight_order.py` is not in the index.
2. Reviewer must read WT contents, not `--cached`. Same mixed-`MM` note as t8b/t10; still uncommitted by instruction.
