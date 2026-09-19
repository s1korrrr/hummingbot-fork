"""Opt-in composition of the complete existing RSI V5 native implementation."""
from typing import ClassVar, Literal

from controllers.directional_trading import rsi_v5
from controllers.generic.modular_spot_core import native_composition

CONFIG_CLASS = 'ModularRSIV5ControllerConfig'
source_root = native_composition.source_root


def source_files():
    return native_composition.source_files('rsi_v5')


class ModularRSIV5ControllerConfig(native_composition.NativeCompositionFields, rsi_v5.RSIv5ControllerConfig):
    controller_name: Literal['modular_rsi_v5'] = 'modular_rsi_v5'
    controller_type: Literal['generic'] = 'generic'
    connector_name: str = 'okx'
    _native_family: ClassVar[str] = 'rsi_v5'

    def get_controller_class(self):
        return ModularRSIV5Controller


class ModularRSIV5Controller(native_composition.NativeComposition, rsi_v5.RSIv5Controller):
    _native_family = 'rsi_v5'
