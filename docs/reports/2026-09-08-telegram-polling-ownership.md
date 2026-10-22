# Telegram polling ownership repair

Scope: local notifier code and offline validation. No bot restart, order, account, credential or configuration change. Existing running processes retain their imported notifier until restarted.

## Cause and behavior

Two local bots could independently call Telegram getUpdates with the same token. Both retried HTTP 409 every five seconds, displacing each other and producing repeated tracebacks.

A per-token advisory file lock now permits one command poller per operating-system user and shared home directory. Other local notifiers wait quietly while continuing to send notifications. Commands describe the bot that currently owns polling; this does not aggregate multiple bots into one command interface. Tokens are hashed for lock names and are never written into lock files. Kernel file locks release on process exit; lock files must not be removed during operation.

Cancellation waits for an already-running urllib request to finish before releasing the lease. Without this drain, a stopped notifier could overlap the next poller despite the lock. The request retains the existing 40-second socket timeout.

HTTP or payload-level getUpdates 409 suspends polling and emits one warning. Notifications remain enabled. The local lease remains held until notifier stop so standby processes do not repeat the remote conflict. Separate hosts, operating-system users or containers without a shared home do not share the lock; remove the competing remote consumer or assign separate Telegram tokens, then restart the notifier. Webhooks are not removed automatically.

## Evidence

The current log contains repeated getUpdates 409 from PID 47162. PIDs 47162 and 47725 were still running after local validation; they were not modified. This patch has not yet been demonstrated against Telegram with these running bots.

The full notifier suite passed: 21 tests, one existing event-loop deprecation warning. Tests cover separate-process ownership, kernel release after child exit, cancellation while an actual worker thread is polling, typed HTTP conflict handling without token disclosure, suspended polling with continued notification sending, and existing transient-error retry behavior. All applicable pre-commit hooks and git diff --check passed.

Command: `/Users/s1kor/anaconda3/envs/hummingbot/bin/python -m pytest test/hummingbot/notifier -q --basetemp=/tmp/telegram-owner-final`.

## Rollback

Revert only the scoped notifier patch while the notifier is stopped. Retain configuration and account state. No database migration is involved. The branch is feat/andrzej_telegram_polling_owner.
