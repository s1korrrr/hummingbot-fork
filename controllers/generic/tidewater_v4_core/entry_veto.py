"""E0 first-entry volatility veto (research candidate, opt-in).

At a first-entry candidate (no inventory, no pending acquisition), E0 refuses the BUY when a
frozen logistic model of recent 1m volatility gives a high probability that the entry is
premature (ADVERSE: the low reaches P(1 - 2g) before the high reaches P(1 + a) within 24 h).

Features, at the signal's closed candle t, from the trailing 1,500 closed 1m closes ending at t:

* x1 = log(EWMA of squared 1m log returns), half-life 60 min, adjust=False, initialized at the
  first squared return of the window;
* x2 = log(sum of squared 1m log returns over the trailing 1,440 minutes).

The model `p = sigmoid(b0 + b1*z1 + b2*z2)` (z standardized by the fit-window mean and
population std) and its threshold were fitted OFFLINE on May-Jul 2026 BNB candidate signals
only; see the root evidence `docs/evidence/2026-10-06-tidewater-candidates/` (`e0_frozen.json`).
The constants are bound into Tidewater's recipe digest when the veto is enabled.

E0 only refuses a BUY admission. It never cancels an order, blocks an exit or a recovery
sell, or demotes a position. A refusal does not consume the cut.
"""
import math
from decimal import Decimal
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import Field, model_validator

from controllers.generic.modular_spot_core.data_reader import dataset_for
from controllers.generic.modular_spot_core.domain import DataUnavailable, PolicyError
from hummingbot.strategy_v2.components.config import PolicyModel

WINDOW = 1500  # closed 1m candles; Meridian's per-pair candle history (max_records)
HALFLIFE = 60  # minutes
SUM_WINDOW = 1440  # minutes
COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']


def e0_features(closes, *, window=WINDOW, halflife=HALFLIFE, sum_window=SUM_WINDOW):
    """(x1, x2) from exactly `window` closes ending at the decision candle t (oldest first)."""
    c = np.asarray(closes, dtype=float)
    if c.ndim != 1 or len(c) != window:
        raise DataUnavailable(f'E0 needs exactly {window} closes')
    if not np.isfinite(c).all() or (c <= 0).any():
        raise PolicyError('E0 closes must be finite and positive')
    r2 = np.diff(np.log(c)) ** 2
    alpha = 1.0 - math.exp(math.log(0.5) / halflife)
    v = float(r2[0])
    for x in r2[1:].tolist():
        v = (1.0 - alpha) * v + alpha * x
    s = float(r2[-sum_window:].sum())
    if not v > 0 or not s > 0:
        raise DataUnavailable('E0 realized variance is zero')
    return math.log(v), math.log(s)


class E0Model(PolicyModel):
    """Frozen E0 coefficients, scaler and threshold. Decimal strings round-trip to the fitted floats."""
    kind: Literal['e0_logistic_vol_v1'] = 'e0_logistic_vol_v1'
    window: Literal[1500] = WINDOW
    halflife_minutes: Literal[60] = HALFLIFE
    sum_window_minutes: Literal[1440] = SUM_WINDOW
    intercept: Decimal
    coef_x1: Decimal
    coef_x2: Decimal
    mean_x1: Decimal
    std_x1: Decimal = Field(gt=0)
    mean_x2: Decimal
    std_x2: Decimal = Field(gt=0)
    threshold: Decimal = Field(gt=0, lt=1)
    fit_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')  # sha256 of the frozen block in e0_frozen.json

    @model_validator(mode='after')
    def finite(self):
        for name in ('intercept', 'coef_x1', 'coef_x2', 'mean_x1', 'mean_x2'):
            if not getattr(self, name).is_finite():
                raise ValueError(f'E0 {name} must be finite')
        return self

    def probability(self, x1, x2):
        z1 = (float(x1) - float(self.mean_x1)) / float(self.std_x1)
        z2 = (float(x2) - float(self.mean_x2)) / float(self.std_x2)
        eta = float(self.intercept) + float(self.coef_x1) * z1 + float(self.coef_x2) * z2
        if eta >= 0:
            return 1.0 / (1.0 + math.exp(-eta))
        e = math.exp(eta)
        return e / (1.0 + e)

    def vetoes(self, p):
        return float(p) > float(self.threshold)


def decision_window(frame, *, candle_open, interval, now, size=WINDOW):
    """The trailing `size` closes ending at the decision candle, and the digest of every closed row up to it.

    Only candles that closed at or before the decision candle's close are read. The digests use
    the runtime's own row hash (`ModularRuntime.observe`), so the caller can prove these are the
    rows the decision consumed.
    """
    if not isinstance(frame, pd.DataFrame) or not set(COLUMNS).issubset(frame.columns):
        raise DataUnavailable('E0 candle columns unavailable')
    closed = frame.loc[(frame.timestamp + interval <= now) & (frame.timestamp <= candle_open), COLUMNS]
    stamps = closed.timestamp.astype(int).tolist()
    digests = dict(zip(stamps, pd.util.hash_pandas_object(closed, index=False).astype(str).tolist()))
    if not stamps or stamps[-1] != int(candle_open):
        raise DataUnavailable('E0 decision candle is not in the closed window')
    if len(stamps) < size:
        raise DataUnavailable(f'E0 needs {size} closed candles, {len(stamps)} available')
    window = closed.iloc[-size:]
    if not (np.diff(window.timestamp.to_numpy()) == interval).all():
        raise DataUnavailable('E0 window is gapped')
    return window.close.to_numpy(dtype=float), digests


def read_frame(runtime):
    """The runtime's candle frame, read the way `ModularRuntime.observe` reads it."""
    c, p = runtime.controller.config, runtime.controller.market_data_provider
    now = p.time()
    if c.data.mode == 'shared':
        if runtime.pool is None:
            raise DataUnavailable('shared candle reader is stopped')
        snapshot = runtime.pool.snapshot(dataset_for(c), now=now, max_age=c.max_candle_age_seconds)
        return snapshot.frame(now=now, max_age=c.max_candle_age_seconds), now
    frame = p.get_candles_df(getattr(c, 'candles_connector', None) or c.connector_name, c.trading_pair, c.interval,
                             c.max_records)
    return frame, now


def evaluate(model: E0Model, runtime, market):
    """E0 at the decision candle of `market`, on the exact closed rows that decision consumed."""
    c = runtime.controller.config
    frame, now = read_frame(runtime)
    closes, digests = decision_window(frame, candle_open=market.candle_open, interval=c.interval_seconds(),
                                      now=now, size=model.window)
    if digests != runtime.decision_inputs(market.candle_open):
        raise DataUnavailable('E0 candles differ from the decision inputs')
    x1, x2 = e0_features(closes, window=model.window, halflife=model.halflife_minutes,
                         sum_window=model.sum_window_minutes)
    p = model.probability(x1, x2)
    return {'x1': repr(x1), 'x2': repr(x2), 'p': repr(p), 'threshold': str(model.threshold),
            'vetoed': model.vetoes(p)}


# Frozen 2026-10-06 by docs/evidence/2026-10-06-tidewater-candidates/freeze_candidates.py (root repo):
# 894 BNB fit origins (508 ADVERSE), fit-set veto rate 0.2002. Values are e0_frozen.json `coefficients`.
E0_FROZEN = E0Model(intercept='0.3090052320373231', coef_x1='-0.09801053710977638', coef_x2='0.7088562914237231',
                    mean_x1='-15.113873748018909', std_x1='0.9104460472587372',
                    mean_x2='-7.84215740675036', std_x2='0.770322432096417', threshold='0.681859486807908',
                    fit_sha256='43618973f9d5b69d946976b7ab6f7d58441c6763543a0255212315fe3ef0fc63')  # noqa: documentation
ENTRY_VETOES = {'e0': E0_FROZEN}
