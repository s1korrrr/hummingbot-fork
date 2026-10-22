from decimal import Decimal as D


from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.decision_protocol import Prerequisite
from controllers.generic.momentum_fusion_v2_core.recipe import MomentumRecipe, scope_for
from controllers.generic.momentum_fusion_v2_core.features import FeaturePipeline

T = 180000


def recipe(variant='B0', **changes):
    values = dict(recipe_id='momentum_v2_fixture', variant=variant, symbol='BNB-USDC',
        datasets=tuple(Dataset(connector='okx', trading_pair='BNB-USDC', interval=i, history=60)
                       for i in ('1m', '5m', '15m')),
        initialization_sha256='a'*64, loss_policy_sha256='b'*64,
        exit_policy_sha256='c'*64, sizing_policy_sha256='d'*64, capital_policy_sha256='e'*64)
    values.update(changes)
    return MomentumRecipe(**values)


def snapshot(dataset, event=T, sequence=1, last=None, epoch='fixture', publication=None):
    end = event // dataset.seconds * dataset.seconds
    rows = []
    for n in range(dataset.history):
        close = D('100') if dataset.interval == '1m' else D('90') + D(n)/10
        rows.append(Candle(open_time=end-(dataset.history-n)*dataset.seconds, open=close,
                           high=close+D('.2'), low=close-D('.2'), close=close, volume=100))
    if last:
        rows[-1] = Candle.model_validate({**rows[-1].model_dump(), **last})
    return CandleSnapshot(dataset=dataset, epoch=epoch, sequence=sequence,
                          published_at=event+1 if publication is None else publication, rows=tuple(rows))


def frames(r, i=0, **kwargs):
    event = T+i*60
    return {d.key: snapshot(d, event, i+1, last=kwargs if d.interval=='1m' else None) for d in r.datasets}


def observed(r, i=0, **kwargs):
    return FeaturePipeline(r).evaluate(frames(r, i, **kwargs), now_ms=(T+i*60+1)*1000)


def scope(r, mode='observation', owner='momentum_v2_fixture'):
    return scope_for(r, program_id='momentum_v2_fixture', owner_id=owner, mode=mode)


def passing():
    return tuple(Prerequisite(name=n, status='PASS', evidence_sha256='f'*64)
                 for n in ('owner_ready', 'risk', 'capital'))
