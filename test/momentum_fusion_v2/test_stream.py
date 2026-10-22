"""One consistent synthetic minute tape; aggregate builders below are fixture-only."""
from decimal import Decimal as D

from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot
from controllers.generic.momentum_fusion_v2_core.features import FeaturePipeline
from controllers.generic.momentum_fusion_v2_core.setups import step, PolicyState
from .fixtures import T, recipe, passing, scope


def prefix_tape():
    rows=[]
    for i in range(900):
        close=D('90')+D(i)/90
        rows.append(Candle(open_time=T-(900-i)*60,open=close,high=close+D('.2'),
                           low=close-D('.2'),close=close,volume=100))
    for i,values in enumerate([
        dict(open=100,high=103.2,low=99.9,close=103,volume=400),
        dict(open=103,high=103.1,low=102.7,close=103,volume=120),
        dict(open=103,high=103.1,low=102.7,close=103,volume=120),
        dict(open=103,high=103.6,low=102.9,close=103.5,volume=400),
        dict(open=103.5,high=103.9,low=103.2,close=103.7,volume=200),
    ]):
        rows.append(Candle(open_time=T+i*60,**values))
    return rows


def snapshots(r, rows, count):
    prefix=rows[:count]; event=prefix[-1].open_time+60; result={}
    for d in r.datasets:
        ratio=d.seconds//60
        end_index=next(i for i in range(len(prefix),0,-1) if (prefix[i-1].open_time+60)%d.seconds==0)
        source=prefix[end_index-d.history*ratio:end_index]
        aggregate=[]
        for n in range(0,len(source),ratio):
            bucket=source[n:n+ratio]
            assert len(bucket)==ratio and bucket[0].open_time%d.seconds==0
            aggregate.append(Candle(open_time=bucket[0].open_time,open=bucket[0].open,
                high=max(v.high for v in bucket),low=min(v.low for v in bucket),close=bucket[-1].close,
                volume=sum(v.volume for v in bucket)))
        result[d.key]=CandleSnapshot(dataset=d,epoch='synthetic_consistent_tape',sequence=count-899,
                                    published_at=event+1,rows=tuple(aggregate))
    return result, (event+1)*1000


def test_consistent_prefix_baseline_and_shelf_have_one_treatment():
    tape=prefix_tape(); outputs={}
    for variant in ('B0','C1'):
        r=recipe(variant); pipeline=FeaturePipeline(r); state=None; decisions=[]
        for count in range(900,906):
            data,now=snapshots(r,tape,count)
            value=pipeline.evaluate(data,now_ms=now)
            state,result=step(r,state,value,scope=scope(r),now_ms=now,prerequisites=passing())
            decisions.append(result)
            state=PolicyState.model_validate_json(state.model_dump_json())
        outputs[variant]=decisions
    assert [i for i,d in enumerate(outputs['B0']) if d.trace.status=='ELIGIBLE_FOR_NATIVE_ADMISSION']==[1]
    assert [i for i,d in enumerate(outputs['C1']) if d.trace.status=='ELIGIBLE_FOR_NATIVE_ADMISSION']==[4]
    assert all(d.trace.execution_authorized is False for group in outputs.values() for d in group)
    assert all(d.economic_backtest is False for group in outputs.values() for d in group)


def test_future_suffix_does_not_change_evaluated_prefix():
    r=recipe('C1'); original=prefix_tape(); changed=prefix_tape()
    changed[-1]=changed[-1].model_copy(update={'high':D(1000),'close':D(999)})
    a,now=snapshots(r,original,904); b,_=snapshots(r,changed,904)
    pipeline=FeaturePipeline(r)
    before=pipeline.evaluate(a,now_ms=now)
    future,future_now=snapshots(r,changed,905)
    pipeline.evaluate(future,now_ms=future_now)
    assert before == pipeline.evaluate(b,now_ms=now)
