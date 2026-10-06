"""Tidewater pace(): per-pair limit and sequential occupancy on a synthetic snapshot."""
from decimal import Decimal as D
from test.tidewater_v4.test_tidewater_runtime import TIGHT, tidewater
from types import SimpleNamespace

from controllers.generic.meridian_v3_recipe import PAIRS
from controllers.generic.tidewater_v4_core.portfolio import TidewaterPortfolio
from controllers.generic.tidewater_v4_core.receipts import ReceiptWriter


class Collecting:
    def __init__(self):
        self.events = []

    def append_rsi_v5_runtime_events(self, events):
        self.events.extend(events)
        return events


def portfolio(config, references):
    owner = SimpleNamespace(config=config, symbol_config=lambda pair: config.symbol_configs()[pair])
    p = TidewaterPortfolio.__new__(TidewaterPortfolio)
    p.owner, p.pacing, p.occupancy = owner, {}, None
    p.receipts = ReceiptWriter(config.id, recorder=Collecting())
    p._reference = lambda pair: references.get(pair)
    return p


def snapshot(basis=None, bid=D('100')):
    basis = basis or {}
    assets = [SimpleNamespace(symbol=pair, bid=bid, basis_quote=D(basis.get(pair, '0')),
                              net_cash_consumed_quote=D(basis.get(pair, '0'))) for pair in PAIRS]
    return SimpleNamespace(assets=assets, commitments=())


def markets():
    # No ATR feature: the live buy gap is its configured base gap (0.8%).
    return {pair: SimpleNamespace(candle_open=1000, bag=None) for pair in PAIRS}


def test_a_pair_at_its_limit_is_refused_while_the_others_pass():
    config = tidewater(tide_gate=None)
    p = portfolio(config, {})
    allowed = p.pace(PAIRS, markets(), snapshot({'BNB-USDC': '300.01'}), 1_000_000)
    assert set(allowed) == set(PAIRS) - {'BNB-USDC'}
    assert p.pacing['BNB-USDC']['outcome'] == 'pair_limit'
    assert [e['event_type'] for e in p.receipts._recorder.events] == ['TIDEWATER_GATE_REFUSED']


def test_six_fee_inclusive_tranches_fit_the_350_limit_and_a_seventh_does_not():
    config = tidewater(tide_gate=None)
    assert config.stage().max_active_tranches == 6
    increment = config.stage().order_quote * (1 + config.risk.fee_reserve_fraction)
    p = portfolio(config, {})
    five = str(5 * increment)
    assert 'BNB-USDC' in p.pace(PAIRS, markets(), snapshot({'BNB-USDC': five}), 1_000_000)
    six = str(6 * increment)
    assert 'BNB-USDC' not in p.pace(PAIRS, markets(), snapshot({'BNB-USDC': six}), 1_000_000)


def test_one_batch_paces_each_candidate_on_occupancy_raised_by_the_earlier_ones():
    # u starts at 0; after the first priority pair is accepted u = 50.15/1000 > 0.05, so the
    # second pair's gap is widened and a one-gap drop (0.8%) is no longer enough.
    config = tidewater(tide_gate=TIGHT)
    references = {pair: D('100') for pair in PAIRS}
    p = portfolio(config, references)
    allowed = p.pace(PAIRS, markets(), snapshot(bid=D('99.2')), 1_000_000)
    assert allowed == ('BTC-USDC',)
    assert p.pacing['ETH-USDC']['outcome'] == 'tide_gate_down'
    assert D(p.pacing['ETH-USDC']['occupancy']) == D('0.05015')


def test_the_bnb_stage_fits_nineteen_fee_inclusive_tranches():
    only_bnb = [{'trading_pair': pair, 'entry_enabled': pair == 'BNB-USDC'} for pair in PAIRS]
    config = tidewater(capital_stage='tidewater_bnb_1000', symbols=only_bnb, tide_gate=None)
    assert config.stage().max_active_tranches == 19
    increment = config.stage().order_quote * (1 + config.risk.fee_reserve_fraction)
    p = portfolio(config, {})
    assert 'BNB-USDC' in p.pace(('BNB-USDC',), markets(), snapshot({'BNB-USDC': str(18 * increment)}), 1)
    assert 'BNB-USDC' not in p.pace(('BNB-USDC',), markets(), snapshot({'BNB-USDC': str(19 * increment)}), 1)
