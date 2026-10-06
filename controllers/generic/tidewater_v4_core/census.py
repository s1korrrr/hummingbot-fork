"""Tidewater signal census ledger (opt-in `census=True`). Report only: no decision reads it.

Population. At the start of every tick, before Meridian's identity and kill-switch holds, its
candidate filters (seed cut, consumed cut, entries disabled), the native flash pause and every
treatment, each pair whose current closed cut fires the BNB MR signal is censused once. Cut
tracking is the ledger's own set of census identities, not Meridian's `_seen`/`_evaluated`.

Identity. `census_id = identity(controller, pair, cut)`. The census record's persistence key is
`tidewater:census:<census_id>`; transitions are `tidewater:outcome:<census_id>:<seq>` (seq 1, 2,
...) and the single final record is `tidewater:final:<census_id>`. Keys never hash a payload, so a
retry or a restart names the same facts.

Outcomes. A transition is written whenever a signal's outcome code differs from its previous one,
so `native_paused -> preview_held -> native_paused` is three transitions. An admission result
settles the signal at once; otherwise the signal is finalized when its pair's cut has passed. The
final code is the last transition, else the pair's decision receipt for that cut, else
`outcome_unrecorded` (stated, never invented).

Recovery. Writes are queued in order and retried each tick until stored; a failed write is
reported in `fault` and never raised into a decision. After a restart the ledger rebuilds from the
durable receipts: censused signals are not re-written, sequence numbers continue, and a signal
whose admission receipt exists but whose outcome was lost is reconciled from that receipt.
"""
from controllers.generic.modular_spot_core.domain import identity

from .receipts import ADMISSION_CODES, ADMITTED, CENSUS, FINAL, OUTCOME, REFUSED, read_receipts


class CensusLedger:
    def __init__(self, controller_id, receipts):
        self.controller_id = controller_id
        self.receipts = receipts
        self.rows = {}  # census_id -> {'pair', 'cut', 'next_seq', 'last', 'finalizing'}
        self.open = set()  # census ids not yet finalized
        self.decisions = {}  # (pair, cut) -> latest REFUSED / ADMITTED code
        self.queue = []  # writes not yet stored, in order
        self.touched = set()  # census ids given an outcome this tick
        self.loaded = False
        self.fault = None
        receipts.listeners.append(self._decided)

    def census_id(self, pair, cut):
        return identity(['tidewater_signal_census', self.controller_id, pair, int(cut)])

    def _decided(self, kind, pair, cut, code):
        if kind in (REFUSED, ADMITTED):
            self.decisions[(pair, cut)] = code

    def _row(self, pair, cut):
        return {'pair': pair, 'cut': int(cut), 'next_seq': 1, 'last': None, 'finalizing': False}

    # ------------------------------------------------------------------ durable state
    def load(self, now):
        """Rebuild once per process from the durable receipts, then reconcile lost admission outcomes."""
        if self.loaded:
            return True
        try:
            records = read_receipts(self.receipts.recorder(), self.controller_id)
        except Exception as exc:  # report only: retried on the next tick
            self.fault = f'census_load:{type(exc).__name__}:{exc}'[:200]
            return False
        for r in records:
            kind = r['kind']
            if kind in (REFUSED, ADMITTED):
                self.decisions[(r['pair'], r['cut'])] = r['code']
            elif kind == CENSUS:
                self.rows[r['census_id']] = self._row(r['pair'], r['cut'])
                self.open.add(r['census_id'])
            elif kind == OUTCOME and r['census_id'] in self.rows:
                row = self.rows[r['census_id']]
                if r['seq'] >= row['next_seq']:
                    row['next_seq'], row['last'] = r['seq'] + 1, r['code']
            elif kind == FINAL and r['census_id'] in self.rows:
                self.rows[r['census_id']]['finalizing'] = True
                self.open.discard(r['census_id'])
        self.loaded = True
        for cid in sorted(self.open):
            row = self.rows[cid]
            settled = self.decisions.get((row['pair'], row['cut']))
            if settled in ADMISSION_CODES and row['last'] != settled:
                self.transition(row['pair'], row['cut'], settled, now=now, reason='reconciled_from_admission_receipt')
            elif settled in ADMISSION_CODES:
                self._finalize(cid, now, reason='reconciled_from_admission_receipt')
        return True

    def flush(self):
        """Store queued writes in order; stop at the first failure (retried on the next tick)."""
        while self.queue:
            kind, pair, cut, code, payload, event_id, now = self.queue[0]
            if not self.receipts.persist(kind, pair=pair, cut=cut, code=code, payload=payload, now=now,
                                         event_id=event_id):
                self.fault = (self.receipts.fault or {}).get('error') or 'census_write_failed'
                return False
            self.queue.pop(0)
        return True

    # ------------------------------------------------------------------ facts
    def observe(self, pair, cut, payload, now):
        """Census one signal unless it already is. `payload` is called only for a new signal."""
        cid = self.census_id(pair, cut)
        if cid in self.rows:
            return cid
        self.rows[cid] = self._row(pair, cut)
        self.open.add(cid)
        self.queue.append((CENSUS, pair, int(cut), 'signal', {'census_id': cid, **payload()},
                           f'tidewater:census:{cid}', now))
        return cid

    def transition(self, pair, cut, code, *, now, reason=None, cp=False):
        """Record the signal's outcome at `cut` if it changed; an admission result settles it."""
        cid = self.census_id(pair, cut)
        row = self.rows.get(cid)
        if row is None or row['finalizing']:
            return
        self.touched.add(cid)
        if code == row['last']:
            return
        seq, previous = row['next_seq'], row['last']
        row['next_seq'], row['last'] = seq + 1, code
        self.queue.append((OUTCOME, pair, row['cut'], code,
                           {'census_id': cid, 'seq': seq, 'previous': previous, 'reason': reason, 'cp_overridden': cp},
                           f'tidewater:outcome:{cid}:{seq}', now))
        if code in ADMISSION_CODES:
            self._finalize(cid, now, reason=reason or 'admission_settled')

    def _finalize(self, cid, now, *, reason):
        row = self.rows[cid]
        if row['finalizing']:
            return
        row['finalizing'] = True
        self.open.discard(cid)
        code = row['last'] or self.decisions.get((row['pair'], row['cut'])) or 'outcome_unrecorded'
        self.queue.append((FINAL, row['pair'], row['cut'], code,
                           {'census_id': cid, 'transitions': row['next_seq'] - 1, 'reason': reason},
                           f'tidewater:final:{cid}', now))

    # ------------------------------------------------------------------ tick
    def begin(self, markets, *, fires, payload, now):
        """Census the raw signal population of this tick, before any hold or treatment."""
        self.touched = set()
        if not self.load(now):
            return
        self.flush()
        for pair in sorted(markets):
            market = markets[pair]
            if market is not None and fires(market):
                self.observe(pair, market.candle_open, lambda pair=pair, market=market: payload(pair, market), now)
        self.flush()

    def end(self, markets, *, held, now):
        """Outcomes for signals no evaluation step reached; finalize signals whose cut has passed."""
        if not self.loaded:
            return
        for pair in sorted(markets):
            market = markets[pair]
            if market is None:
                continue
            cid = self.census_id(pair, market.candle_open)
            if cid in self.open and cid not in self.touched:
                code = held(pair, market.candle_open)
                if code is not None:
                    self.transition(pair, market.candle_open, code, now=now)
        for cid in sorted(self.open):
            row = self.rows[cid]
            market = markets.get(row['pair'])
            if market is not None and market.candle_open != row['cut']:
                self._finalize(cid, now, reason='cut_passed')
        self.flush()

    def report(self):
        return {'loaded': self.loaded, 'open': len(self.open), 'queued': len(self.queue), 'fault': self.fault}
