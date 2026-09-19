"""Opt-in composition of the complete existing OK RSI native implementation."""
from typing import ClassVar, Literal

from controllers.directional_trading import ok_rsi
from controllers.directional_trading.ok_rsi_core.config import OKRSIControllerConfig
from controllers.generic.modular_spot_core import native_composition

CONFIG_CLASS = 'ModularOKRSIControllerConfig'
source_root = native_composition.source_root


def source_files():
    return native_composition.source_files('ok_rsi')


class ModularOKRSIControllerConfig(native_composition.NativeCompositionFields, OKRSIControllerConfig):
    controller_name: Literal['modular_ok_rsi'] = 'modular_ok_rsi'
    controller_type: Literal['generic'] = 'generic'
    _native_family: ClassVar[str] = 'ok_rsi'

    def get_controller_class(self):
        return ModularOKRSIController


class ModularOKRSIController(native_composition.NativeComposition, ok_rsi.OKRSIController):
    _native_family = 'ok_rsi'
