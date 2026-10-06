"""Tidewater decision receipts: why a candidate was refused, admitted or revoked.

Receipts go to the recorder's sequenced, append-only controller event store
(`MarketsRecorder.append_rsi_v5_runtime_events`) under Tidewater's controller id, with
`TIDEWATER_` event types. Tidewater reads them with `read_receipts`; the RSI V5 runtime
replay never reads them.

Timing. A gate refusal has no other effect, so its receipt is written before the refusal
returns. A revocation receipt is written before `entry_permission` returns False for an entry
that still has an open order to end (an effective acquisition denial). Post-entry permission
checks, after a full fill or after the entry already ended, cancel nothing and get no receipt. An
admission receipt follows the native target and grant commits and carries the target hash
and grant ids, so a committed target without its receipt is a detectable gap.

Failure. Writing never raises into a decision and never blocks an order. A failure sets
`fault` with the interval and count of failed writes (reported by the controller) and the
receipt is lost; complete reconstruction is not guaranteed for that interval.
"""
import json

from controllers.generic.modular_spot_core.domain import identity
from controllers.generic.modular_spot_core.native_bindings import NativeBindings

REFUSED, ADMITTED, REVOKED = 'TIDEWATER_GATE_REFUSED', 'TIDEWATER_ADMITTED', 'TIDEWATER_ENTRY_REVOKED'
# Research candidates (written only when a candidate is enabled):
# * CENSUS: one record per first-entry candidate, written before any treatment; its id is a
#   deterministic function of (controller, pair, cut) and its payload of the candle, so a
#   re-evaluated cut or a restart rewrites the same fact;
# * SHADOW_TIDE_GATE: what the tide gate would have said about a candidate the entry veto refused.
#   Never read by a decision;
# * CAPITULATION: the capitulation override lifted the native flash pause for this candidate; it
#   carries what the pause said.
# A candidate's receipts share the episode `pair:cut`. Its outcome is the existing vocabulary: a
# REFUSED receipt (an entry-veto refusal uses code `entry_veto_e0`, `entry_veto_x0` or
# `entry_veto_unavailable`)
# or, once the target and grants commit, an ADMITTED receipt with code `admitted`, `not_admitted`
# or `admission_unresolved`.
CENSUS, SHADOW_TIDE_GATE = 'TIDEWATER_FIRST_ENTRY_CENSUS', 'TIDEWATER_SHADOW_TIDE_GATE'
CAPITULATION = 'TIDEWATER_CAPITULATION_OVERRIDE'
KINDS = frozenset({REFUSED, ADMITTED, REVOKED, CENSUS, SHADOW_TIDE_GATE, CAPITULATION})
# Closed list of gate refusal codes. A revocation receipt names its failing predicate.
REFUSAL_CODES = frozenset({'tide_gate_down', 'tide_gate_up_occupancy', 'tide_gate_up_gap', 'pair_limit',
                           'pacing_unavailable', 'entry_veto_e0', 'entry_veto_x0', 'entry_veto_unavailable'})
WRITE_ERRORS = (Exception,)  # any store failure is reported, never raised into a decision


class ReceiptWriter:
    def __init__(self, controller_id, *, recorder=None):
        self.controller_id = controller_id
        self._recorder = recorder
        self._seen = set()  # (pair, cut, kind, code) already written by this process
        self.fault = None
        self.written = 0

    def recorder(self):
        return self._recorder if self._recorder is not None else NativeBindings.recorder()

    def record(self, kind, *, pair, cut, code, payload, now, dedupe=None):
        """Write one receipt unless this process already wrote the same (pair, cut, kind, code, dedupe).

        `dedupe` names what makes two receipts different facts (a revocation's predicate). Gate
        refusals deduplicate on their code: a changed code is a new receipt, a changed
        occupancy under the same code is not.
        """
        if kind not in KINDS:
            raise ValueError(f'unknown Tidewater receipt kind {kind}')
        if kind == REFUSED and code not in REFUSAL_CODES:
            raise ValueError(f'unknown Tidewater refusal code {code}')
        key = (pair, cut, kind, code, dedupe)
        if key in self._seen:
            return False
        body = {'pair': pair, 'cut': cut, 'code': code, **payload}
        payload_json = json.dumps(body, sort_keys=True, default=str, separators=(',', ':'))
        event = {'controller_id': self.controller_id,
                 'event_id': 'tidewater:' + identity([self.controller_id, kind, payload_json]),
                 'episode_id': f'{pair}:{cut}', 'event_type': kind, 'payload_json': payload_json,
                 'created_at': float(now)}
        try:
            recorder = self.recorder()
            if recorder is None:
                raise RuntimeError('native recorder unavailable')
            recorder.append_rsi_v5_runtime_events([event])
        except WRITE_ERRORS as exc:
            previous = self.fault or {'first_at': now, 'count': 0}
            self.fault = {'first_at': previous['first_at'], 'last_at': now, 'count': previous['count'] + 1,
                          'kind': kind, 'pair': pair, 'error': f'{type(exc).__name__}:{exc}'[:200]}
            return False
        self._seen.add(key)
        self.written += 1
        return True


def read_receipts(recorder, controller_id):
    """Tidewater receipts of one controller in sequence order, as dicts."""
    rows = recorder.get_rsi_v5_runtime_events(controller_id)
    return [{'sequence': r.sequence, 'kind': r.event_type, 'episode': r.episode_id,
             'created_at': r.created_at, **json.loads(r.payload_json)}
            for r in rows if r.event_type in KINDS]
