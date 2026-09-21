import pytest

try:
    from hummingbot.connector.connector_base import ConnectorBase  # noqa: F401
except ModuleNotFoundError:
    pytest.skip('compiled hummingbot native package required', allow_module_level=True)

from controllers.generic.momentum_fusion_v2 import MomentumFusionV2Config
from hummingbot.strategy_v2.components.decision_protocol import (
    DecisionScope,
    ObservationRef,
    Opportunity,
    Prerequisite,
    decide,
)


def test_disabled_execution_and_live_are_rejected_at_construction():
    assert MomentumFusionV2Config(id='mom').execution_enabled is False
    with pytest.raises(ValueError, match='live'):
        MomentumFusionV2Config(id='mom', execution_enabled=True, execution_mode='live')
    with pytest.raises(ValueError):
        MomentumFusionV2Config(id='mom', trade_quote=100, total_amount_quote=10)


def test_expired_or_foreign_scope_is_not_admitted():
    scope = DecisionScope(program_id='okx_spot_momentum_v2', owner_id='mom', connector='okx',
                          trading_pair='BNB-USDC', recipe_sha256='a' * 64)
    ref = ObservationRef(dataset_sha256='b' * 64, epoch='e', revision=1, content_sha256='c' * 64,
                         event_time_ms=1000, available_time_ms=1000)
    opp = Opportunity(scope=scope, setup='b0_breakout', action='acquire', primary=ref,
                      created_at_ms=1000, lifetime_ms=100)
    checks = (Prerequisite(name='data', status='PASS', evidence_sha256='d' * 64),)
    cursor, _ = decide(None, opp, now_ms=1000, prerequisites=checks)
    later = Opportunity(scope=scope, setup='b0_breakout', action='acquire',
                        primary=ref.model_copy(update={'event_time_ms': 2000, 'available_time_ms': 2000,
                                                       'content_sha256': 'e' * 64}),
                        created_at_ms=2000, lifetime_ms=100)
    _, expired = decide(cursor, later, now_ms=2200, prerequisites=checks)
    assert expired.status == 'HOLD'
    other = DecisionScope(program_id='okx_spot_momentum_v2', owner_id='other', connector='okx',
                          trading_pair='BNB-USDC', recipe_sha256='a' * 64)
    foreign = Opportunity(scope=other, setup='b0_breakout', action='acquire',
                          primary=later.primary, created_at_ms=2000, lifetime_ms=100)
    with pytest.raises(ValueError):
        decide(cursor, foreign, now_ms=2000, prerequisites=checks)
