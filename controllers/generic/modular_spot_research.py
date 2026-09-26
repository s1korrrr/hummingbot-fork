"""Offline research binding for the component composition; never a deployment profile.

`ModularSpotResearchConfig` admits `ModularSpotController` to the native paper
session: the isolated `okx_paper_trade` connector executes, the public `okx`
namespace supplies injected candles, and every native executor, recorder, fee
and inventory rule is inherited unchanged. The binding adds no strategy
parameter, no second ledger and no live route. A passing paper replay is an
engineering result, not economic evidence or activation authority.
"""
from pathlib import Path
from typing import Literal

from pydantic import ConfigDict, model_validator

from controllers.generic import modular_spot
from controllers.generic.modular_spot import ModularSpotController, ModularSpotControllerConfig
from controllers.generic.modular_spot_core.domain import PolicyError
from controllers.generic.modular_spot_core.native_bindings import NativeBindings
from controllers.generic.modular_spot_core.recipe_contract import source_records
from hummingbot.connector.exchange.paper_trade.native_paper_trade_exchange import is_native_paper_connector
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.components.source_identity import component_source_files

CONFIG_CLASS = 'ModularSpotResearchConfig'


def source_root():
    return Path(__file__).parent


def source_files():
    return [Path(__file__), *modular_spot.source_files()]


class ModularSpotResearchConfig(ModularSpotControllerConfig):
    controller_name: Literal['modular_spot_research'] = 'modular_spot_research'
    research_only: Literal[True] = True
    execution_mode: Literal['paper'] = 'paper'
    connector_name: Literal['okx_paper_trade'] = 'okx_paper_trade'
    candles_connector: Literal['okx'] = 'okx'
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False, validate_default=True)

    @model_validator(mode='after')
    def research_scope(self):
        if self.data.mode != 'native':
            raise ValueError('offline research binding reads injected native candles only; shared transport opens sockets')
        if self.universe.mode != 'fixed':
            raise ValueError('offline research binding admits the fixed single-pair universe only; '
                             'sell_only adopts inventory and impulse needs shared screening data')
        if self.budget is None or self.account == 'unassigned':
            raise ValueError('research composition requires an explicit program budget and account; '
                             'an unfunded topology would replay without ever trading')
        if self.recipe_binding is not None:
            raise ValueError('recipe bindings verify the deployment source closure, which excludes this binding module')
        return self

    def source_manifest(self):
        return source_records(source_root(), source_files())

    def get_controller_class(self):
        return ModularSpotResearchController


class ResearchPaperBindings(NativeBindings):
    """Market snapshots come from the isolated native paper connector; all other effects are inherited."""

    @staticmethod
    def spot_connector(controller):
        c, p = controller.config, controller.market_data_provider
        connector = (getattr(p, 'connectors', None) or {}).get(c.connector_name)
        if not is_native_paper_connector(connector):
            raise PolicyError('research snapshot requires the isolated native paper connector')
        return connector


class ModularSpotResearchController(ModularSpotController):
    allow_implementation_hot_reload = False

    @classmethod
    def implementation_source_files(cls):
        return [*source_files(), *component_source_files()]

    def __init__(self, config, market_data_provider, actions_queue, **kwargs):
        if type(config) is not ModularSpotResearchConfig:
            raise ValueError('exact research configuration required')
        connector = (getattr(market_data_provider, 'connectors', None) or {}).get(config.connector_name)
        if not is_native_paper_connector(connector):
            raise ValueError('research composition requires the isolated native paper connector')
        super().__init__(config, market_data_provider, actions_queue, **kwargs)

    def _runtime_bindings(self):
        return ResearchPaperBindings()

    def get_candles_config(self):
        return [CandlesConfig(connector=self.config.candles_connector, trading_pair=self.config.trading_pair,
                              interval=self.config.interval, max_records=self.config.max_records)]

    def get_custom_info(self) -> dict:
        return {**super().get_custom_info(), 'execution_mode': self.config.execution_mode,
                'research_only': self.config.research_only}

    def to_format_status(self):
        return [*super().to_format_status(), 'Offline research binding: paper connector, injected candles.']
