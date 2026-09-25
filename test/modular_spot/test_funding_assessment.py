"""Readiness inspects real native funding without consuming or releasing capital."""
from test.hummingbot.strategy_v2.paper.test_paper_funding import funded_config, paper

import pytest

from controllers.generic.modular_spot_core.native_funding import NativeFundingStore
from hummingbot.connector.markets_recorder import MarketsRecorder
from hummingbot.model.metadata import Metadata
from hummingbot.strategy_v2.paper_session import NativePaperSession


@pytest.mark.asyncio
async def test_assessment_is_read_only_and_exposes_binding_failure_before_signal(tmp_path):
    previous = MarketsRecorder._shared_instance
    MarketsRecorder._shared_instance = None
    session = NativePaperSession(paper(), [funded_config()], tmp_path / 'assessment.sqlite')
    try:
        owner = next(iter(session.controllers.values()))
        store = NativeFundingStore(owner, session.recorder)
        with session.recorder.sql_manager.get_new_session() as sql:
            before = [(row.key, row.value) for row in sql.query(Metadata).all()]
        result = store.assess()
        assert result['available_quote'] == '50'
        assert result['pending_quote'] == '0'
        assert result['assessment'] == 'read_only_native_evidence'
        owner._funding_hold = 'Native minimum exceeds funded per-entry cap'
        owner._refresh_funding_assessment()
        assert owner._funding_hold == 'Native minimum exceeds funded per-entry cap'
        assert owner.get_custom_info()['entry_admission']['state'] == 'blocked'
        owner._funding_hold = None
        owner._funding_assessed_at = None
        with session.recorder.sql_manager.get_new_session() as sql:
            assert [(row.key, row.value) for row in sql.query(Metadata).all()] == before
        raw, state = store._load()
        assert store._write(raw, state)
        owner._composition_digest = 'e' * 64  # a trading-configuration change is never carried forward
        owner._funding_store = None
        owner._refresh_funding_assessment()
        assert owner._funding_assessment_hold  # configuration change: refused, never carried forward
        assert owner.get_custom_info()['entry_admission']['state'] == 'blocked'
        assert session.recorder.get_all_executors() == []
        assert session._connector.in_flight_orders == {}
    finally:
        await session.close()
        MarketsRecorder._shared_instance = previous
