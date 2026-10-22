"""RSI Modular: one public controller entrypoint, explicit native policy profiles.

Each profile creates one native controller instance. There is no outer controller,
second lifecycle, hybrid strategy or inferred profile default.
"""
from types import MappingProxyType
from typing import ClassVar, Literal

from pydantic import Field

from controllers.directional_trading import ok_rsi, rsi_v5
from controllers.generic.modular_spot_core import native_composition

source_root = native_composition.source_root


class _ProfileFields(native_composition.NativeCompositionFields):
    controller_name: Literal['rsi_modular'] = 'rsi_modular'
    controller_type: Literal['generic'] = 'generic'
    _public_module: ClassVar[str] = 'rsi_modular'

    def set_id(self, id_value=None):
        from hummingbot.strategy_v2.utils.common import generate_unique_id
        return id_value if id_value is not None else 'modular_spot_' + generate_unique_id()


class _OKRSIProfileConfig(_ProfileFields, ok_rsi.OKRSIControllerConfig):
    controller_name: Literal['rsi_modular'] = 'rsi_modular'
    controller_type: Literal['generic'] = 'generic'
    profile: Literal['ok_rsi'] = Field(json_schema_extra={'is_updatable': False})
    _native_family: ClassVar[str] = 'ok_rsi'
    _public_module: ClassVar[str] = 'rsi_modular'

    def get_controller_class(self):
        return _OKRSIProfileController


class _RSIV5ProfileConfig(_ProfileFields, rsi_v5.RSIv5ControllerConfig):
    controller_name: Literal['rsi_modular'] = 'rsi_modular'
    controller_type: Literal['generic'] = 'generic'
    profile: Literal['rsi_v5'] = Field(json_schema_extra={'is_updatable': False})
    connector_name: str = 'okx'
    _native_family: ClassVar[str] = 'rsi_v5'
    _public_module: ClassVar[str] = 'rsi_modular'

    def get_controller_class(self):
        return _RSIV5ProfileController


class _OKRSIProfileController(native_composition.NativeComposition, ok_rsi.OKRSIController):
    allow_implementation_hot_reload = False
    _native_family = 'ok_rsi'
    _public_module = 'rsi_modular'


class _RSIV5ProfileController(native_composition.NativeComposition, rsi_v5.RSIv5Controller):
    allow_implementation_hot_reload = False
    _native_family = 'rsi_v5'
    _public_module = 'rsi_modular'


CONFIG_PROFILES = MappingProxyType({'ok_rsi': _OKRSIProfileConfig, 'rsi_v5': _RSIV5ProfileConfig})


def get_config_class(profile):
    if type(profile) is not str or profile not in CONFIG_PROFILES:
        raise ValueError('RSI Modular requires explicit profile: ok_rsi or rsi_v5')
    return CONFIG_PROFILES[profile]


def source_files(profile):
    get_config_class(profile)
    return native_composition.source_files(profile, public_module='rsi_modular')
