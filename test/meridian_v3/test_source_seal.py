"""F01: one explicit executable source closure for the unified Meridian owner.

Mutations are applied to the bytes the seal reads, never to files on disk.
"""
import hashlib
from pathlib import Path
from test.meridian_v3.test_unified_runtime import unified

import pytest

from controllers.generic import meridian_v3, meridian_v3_portfolio, modular_spot
from controllers.generic.meridian_v3_portfolio import MeridianPortfolioRecipe
from controllers.generic.meridian_v3_unified_core import config as unified_config
from controllers.generic.meridian_v3_unified_core.config import MeridianUnifiedConfig
from hummingbot.strategy_v2.components import source_identity as identity_module
from hummingbot.strategy_v2.components.recipe_contract import digest, source_digest
from hummingbot.strategy_v2.controllers.portfolio_target_store_v3 import NativeTargetStore, ProposalWriter

ROOT = Path(unified_config.__file__).resolve().parents[3]
PORTFOLIO_V3 = ROOT / 'hummingbot/strategy_v2/portfolio_v3'
CONSEQUENTIAL = (
    'hummingbot/strategy_v2/portfolio_v3/allocation.py',
    'hummingbot/strategy_v2/portfolio_v3/capital.py',
    'hummingbot/strategy_v2/portfolio_v3/contracts.py',
    'hummingbot/strategy_v2/portfolio_v3/runtime_contracts_v1.py',
    'hummingbot/strategy_v2/controllers/native_portfolio_bridge_v3.py',
    'hummingbot/strategy_v2/controllers/portfolio_target_store_v3.py',
)
PORTFOLIO_OWNED = (*CONSEQUENTIAL, 'controllers/generic/meridian_v3_portfolio.py',
                   'controllers/generic/meridian_v3_recipe.py')
UNIFIED_ONLY = ('controllers/generic/meridian_v3_unified.py',
                'controllers/generic/meridian_v3_unified_core/config.py',
                'controllers/generic/meridian_v3_unified_core/portfolio.py',
                'controllers/generic/meridian_v3_unified_core/symbols.py',
                'controllers/generic/meridian_v3_unified_core/execution.py',
                'hummingbot/core/data_type/order_book_observation.py',
                'hummingbot/core/data_type/order_book_message.py',
                'hummingbot/core/data_type/order_book_tracker.py',
                'hummingbot/connector/exchange_py_base.py',
                'hummingbot/connector/exchange/okx/okx_api_order_book_data_source.py',
                'hummingbot/connector/exchange/paper_trade/native_paper_trade_exchange.py',
                'hummingbot/data_feed/market_context_client.py',
                'hummingbot/strategy_v2/executors/order_accounting.py',
                'hummingbot/strategy_v2/executors/position_executor/position_executor.py',
                'hummingbot/strategy_v2/executors/executor_orchestrator.py')


@pytest.fixture
def native(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from hummingbot.model import get_declarative_base
    from hummingbot.model.metadata import Metadata
    get_declarative_base()
    engine = create_engine('sqlite:///' + str(tmp_path / 'native.sqlite'))
    Metadata.__table__.create(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


def mutate(monkeypatch, relative):
    """Every seal reads source bytes through checked_source; append one byte to one file."""
    target = (ROOT / relative).resolve()
    original = identity_module.checked_source

    def mutated(path):
        data = original(path)
        return data + b'\n# mutation\n' if Path(path).resolve() == target else data
    monkeypatch.setattr(identity_module, 'checked_source', mutated)
    monkeypatch.setattr(unified_config, '_SOURCE_MANIFEST', {})


def unified_source(config):
    return digest(config.source_manifest())


def portfolio_recipe(config):
    return MeridianPortfolioRecipe(config.mandate(), config.p0_policy()).recipe_sha256


def legacy_identities():
    from hummingbot.strategy_v2.components.source_identity import component_root, component_source_files
    root = component_root()
    components = digest([[p.relative_to(root).as_posix(), hashlib.sha256(identity_module.checked_source(p)).hexdigest()]
                         for p in component_source_files()])
    return {'components': components,
            'modular_spot': source_digest(modular_spot.source_root(), modular_spot.source_files()),
            'meridian_v3_composition': source_digest(Path(meridian_v3.__file__).parent, meridian_v3.source_files()),
            'symbol_fingerprint': unified().symbol_configs()['BTC-USDC'].fingerprint()}


def test_closure_lists_every_consequential_module_explicitly():
    listed = {p.resolve().relative_to(ROOT).as_posix() for p in unified_config.source_files()}
    sealed = {name for name, _ in unified().source_manifest()}
    portfolio = {p.resolve().relative_to(ROOT).as_posix() for p in meridian_v3_portfolio.portfolio_source_files()}
    for name in (*CONSEQUENTIAL, *UNIFIED_ONLY):
        assert name in listed and name in sealed, name
    for name in PORTFOLIO_OWNED:
        assert name in portfolio, name
    # A new portfolio_v3 module cannot silently fall outside the seal.
    package = {p.relative_to(ROOT).as_posix() for p in PORTFOLIO_V3.glob('*.py')}
    assert package <= portfolio and package <= sealed
    assert len(listed) == len(unified_config.source_files())  # no duplicate sources


@pytest.mark.parametrize('relative', (*PORTFOLIO_OWNED, *UNIFIED_ONLY))
def test_changing_any_sealed_file_changes_the_unified_source_digest(monkeypatch, relative):
    config = unified()
    before = unified_source(config), config.seal()
    mutate(monkeypatch, relative)
    assert unified_source(config) != before[0]
    assert config.seal().source_sha256 != before[1].source_sha256
    # Behavioural configuration and the source-free recipe digest are unchanged by code.
    assert config.seal().configuration_sha256 == before[1].configuration_sha256
    assert config.seal().recipe_sha256 == before[1].recipe_sha256


@pytest.mark.parametrize('relative', PORTFOLIO_OWNED)
def test_changing_a_portfolio_file_changes_the_portfolio_recipe_and_writer_epoch(monkeypatch, relative):
    config = unified()
    before = portfolio_recipe(config)
    mutate(monkeypatch, relative)
    assert portfolio_recipe(config) != before


@pytest.mark.parametrize('relative', CONSEQUENTIAL)
def test_sealed_binding_rejects_a_changed_consequential_module(monkeypatch, relative):
    sealed = unified(execution_enabled=True).model_dump(mode='json')
    MeridianUnifiedConfig(**sealed)
    mutate(monkeypatch, relative)
    with pytest.raises(ValueError, match='implementation source changed'):
        MeridianUnifiedConfig(**sealed)


@pytest.mark.parametrize('relative', ('hummingbot/strategy_v2/controllers/native_portfolio_bridge_v3.py',
                                      'hummingbot/strategy_v2/portfolio_v3/runtime_contracts_v1.py'))
def test_populated_target_store_rejects_a_writer_from_changed_source(monkeypatch, native, relative):
    from test.portfolio_v3.test_meridian_composition import composition, observation
    from test.portfolio_v3.test_portfolio import proposal, snapshot

    from controllers.generic.meridian_v3_recipe import PAIRS

    def writer(recipe):
        return ProposalWriter(portfolio_id='meridian_v3', account_id='paper_only', writer_id='w', epoch='e1', fence=1,
                              mandate_sha256=recipe.mandate.identity, recipe_sha256=recipe.recipe_sha256)
    recipe = composition()
    template = snapshot().assets[0]
    s = snapshot().model_copy(update={'mandate_sha256': recipe.mandate.identity, 'entry_universe': PAIRS,
                                      'assets': tuple(template.model_copy(update={'symbol': p}) for p in PAIRS)})
    o = proposal(symbol='BTC-USDC', producer='bnb_p23_mr').opportunity
    o = o.model_copy(update={'scope': o.scope.model_copy(update={'recipe_sha256': recipe.recipe_sha256})})
    target = recipe.propose(s, (observation(o),), epoch='e1', fence=1)
    NativeTargetStore(native, writer(recipe)).publish(target, previous_sha256=None, now_ms=1000)
    mutate(monkeypatch, relative)
    changed = composition()
    assert changed.recipe_sha256 != recipe.recipe_sha256
    with pytest.raises(ValueError, match='another writer or recipe'):
        NativeTargetStore(native, writer(changed)).read()


@pytest.mark.parametrize('relative', (*CONSEQUENTIAL, *UNIFIED_ONLY))
def test_the_legacy_component_and_p23_seals_do_not_move(monkeypatch, relative):
    """The closure is additive: legacy recipe and symbol decision identities stay identical."""
    before = legacy_identities()
    mutate(monkeypatch, relative)
    assert legacy_identities() == before


def test_legacy_component_seal_excludes_the_portfolio_modules():
    from hummingbot.strategy_v2.components.source_identity import component_source_files
    components = {p.resolve() for p in component_source_files()}
    assert not components & {(ROOT / name).resolve() for name in CONSEQUENTIAL}
    rows = {name for name, _ in unified().symbol_configs()['BTC-USDC'].source_manifest()}
    assert not rows & set(CONSEQUENTIAL)
