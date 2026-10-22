"""Explicit synthetic native snapshots; not cached exchange or economic evidence."""
from decimal import Decimal

from hummingbot.strategy_v2.components.candle_data import Candle, CandleSnapshot, Dataset
from hummingbot.strategy_v2.components.decision_protocol import DecisionScope
from hummingbot.strategy_v2.components.recipe_contract import RecipeBinding, digest


def ranged_snapshot(*, bars=60, delay_ms=7):
    dataset = Dataset(connector='okx', trading_pair='ETH-USDC', history=60)
    rows = []
    for i in range(bars):
        close = Decimal(100 + i)
        rows.append(Candle(
            open_time=i * 60, open=close, high=close + Decimal('2'),
            low=close - Decimal('1'), close=close, volume=1))
    close_s = bars * 60
    return CandleSnapshot(
        dataset=dataset, epoch='fixture-a', sequence=1,
        published_at=close_s + delay_ms / 1000, rows=tuple(rows))


def tape(prices=(100, 98, 102, 101, 103), delay_ms=7):
    dataset = Dataset(connector='okx', trading_pair='ETH-USDC', history=60)
    rows = [Candle(open_time=i * 60, open=100, high=100, low=100, close=100, volume=1) for i in range(60)]
    snapshots = []
    for i, value in enumerate(prices):
        c = Decimal(str(value))
        rows.append(Candle(open_time=(60 + i) * 60, open=c, high=c, low=c, close=c, volume=1))
        snapshots.append(CandleSnapshot(
            dataset=dataset, epoch='fixture-a', sequence=i + 1,
            published_at=(61 + i) * 60 + delay_ms / 1000, rows=tuple(rows[-60:])))
    return snapshots


def contract(binding, snapshots=None, source_sha256=None, setup='dip-1'):
    snapshots = snapshots or tape()
    s = snapshots[0]
    ref = binding.observation_ref(s)
    recipe = RecipeBinding(recipe_id='nre_fixture', recipe_sha256=digest('recipe'),
                           source_sha256=source_sha256 or digest('synthetic-source'), configuration_sha256=digest('config'))
    scope = DecisionScope(program_id='fixture_nre_v2', owner_id='nre_fixture_owner', connector='okx',
                          trading_pair='ETH-USDC', recipe_sha256=recipe.recipe_sha256)
    return binding.PathContract(scope=scope, recipe=recipe, dataset=s.dataset, setup=setup,
                                setup_event_ms=ref.event_time_ms, setup_available_ms=ref.available_time_ms,
                                setup_evidence_sha256=digest('setup-evidence'), frozen_scale=2.0, scale_parent=ref,
                                scale_implementation_sha256=digest('atr-implementation'),
                                initialization_sha256=digest('atr-initialization'),
                                policy=binding.path.Policy(2, 1.0, 0.5, 20), max_transport_age_ms=15000)
