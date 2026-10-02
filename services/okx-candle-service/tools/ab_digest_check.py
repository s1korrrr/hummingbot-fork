#!/usr/bin/env python3
"""Active/active comparison of two okx-candle-service status files (`--status-file` output).

Pure function `compare` returns findings; the CLI prints them as JSON and exits non-zero when any
finding is critical. Alarm codes map 1:1 onto `scripts/stack_alert_observation.check(service, code, ...)`.
"""
import json
import sys

STALE_CLOSE_AGE_S = 120.0      # newest closed bar older than this: the instance is not serving a live market
STATUS_MAX_AGE_S = 30.0        # status file older than this: the instance (or its writer) is down
HEAD_LAG_BARS = 1              # instances may differ by one bar while a minute rolls over
REJECT_WARN_PER_PAIR = 5       # rejected rows per pair over the observation window


def finding(code, severity, pair, detail):
    return {'code': code, 'severity': severity, 'pair': pair, 'detail': detail}


def compare(primary, secondary, now):
    out = []
    for label, view in (('primary', primary), ('secondary', secondary)):
        if view is None or now - view.get('published_at', 0) > STATUS_MAX_AGE_S:
            out.append(finding('candle_instance_status_stale', 'critical', None, f'{label} status file missing or older than {STATUS_MAX_AGE_S:.0f}s'))
    if primary is None or secondary is None:
        return out
    for pair in sorted(set(primary['pairs']) & set(secondary['pairs'])):
        a, b = primary['pairs'][pair], secondary['pairs'][pair]
        ready = [(label, v) for label, v in (('primary', a), ('secondary', b)) if v['ready']]
        if not ready:
            out.append(finding('candle_pair_unready', 'critical', pair, f"neither instance ready ({a['reason']!r} / {b['reason']!r})"))
            continue
        if len(ready) == 1:
            out.append(finding('candle_instance_degraded', 'warning', pair, f'only {ready[0][0]} is ready; redundancy lost'))
        for label, v in ready:
            age = v.get('last_close_age_s')
            if age is None or age > STALE_CLOSE_AGE_S:
                out.append(finding('candle_pair_stale', 'critical', pair, f'{label} newest closed bar age {age}'))
        for label, v in (('primary', a), ('secondary', b)):
            if sum(v.get('rejected', {}).values()) >= REJECT_WARN_PER_PAIR:
                out.append(finding('candle_rejections', 'warning', pair, f"{label} rejected {v['rejected']}"))
        if len(ready) == 2:
            ha, hb = a['window_head_open_time'], b['window_head_open_time']
            if ha is None or hb is None:
                continue
            if abs(ha - hb) > 60 * HEAD_LAG_BARS:
                out.append(finding('candle_pair_head_lag', 'critical', pair, f'heads differ: primary {ha} secondary {hb}'))
            elif ha == hb and a['window'] == b['window'] and a['window_digest'] != b['window_digest']:
                out.append(finding('candle_pair_digest_mismatch', 'critical', pair,
                                   f"same head {ha}, same {a['window']}-bar window, different digests"))
    return out


def load(path):
    try:
        with open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return None


def main(argv=None):
    import time
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        print('usage: ab_digest_check.py PRIMARY_STATUS.json SECONDARY_STATUS.json', file=sys.stderr)
        return 2
    findings = compare(load(argv[0]), load(argv[1]), time.time())
    json.dump({'findings': findings}, sys.stdout, indent=2)
    sys.stdout.write('\n')
    return 1 if any(f['severity'] == 'critical' for f in findings) else 0


if __name__ == '__main__':
    sys.exit(main())
