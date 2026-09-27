"""Immutable recipe identities. These records never grant execution authority."""
import json
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Mapping

from pydantic import Field, field_validator

from .config import PolicyModel
from .domain import PolicyError

CONTROL_FIELDS = frozenset({'recipe_binding', 'execution_enabled', 'manual_kill_switch', 'acquisition_enabled'})

# Configuration fields added after live owners bound their identity, with the
# default value that preserves the earlier behaviour. At exactly that value the
# field is left out of the configuration digest, so a code upgrade that only
# adds the field keeps existing funding ledgers and held-recovery checkpoints
# bound. Any other value is part of the identity. A new configuration field
# belongs here when its default preserves the previous behaviour; otherwise the
# upgrade needs explicit reconciliation.
IDENTITY_NEUTRAL_DEFAULTS = MappingProxyType({
    'recovery_keep_trail_after_target': False,
})


def _identity_neutral(key: str, value) -> bool:
    if key not in IDENTITY_NEUTRAL_DEFAULTS:
        return False
    default = IDENTITY_NEUTRAL_DEFAULTS[key]
    return type(value) is type(default) and value == default


def canonical_json(value) -> str:
    def encode(obj):
        if isinstance(obj, Decimal):
            if not obj.is_finite():
                raise ValueError('nonfinite values cannot be sealed')
            return str(obj)
        if isinstance(obj, PolicyModel):
            return obj.model_dump(mode='json')
        raise TypeError(f'unsupported contract value: {type(obj).__name__}')
    return json.dumps(value, default=encode, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value) -> str:
    return sha256(canonical_json(value).encode('utf-8')).hexdigest()


def configuration_digest(payload: Mapping) -> str:
    if not isinstance(payload, Mapping) or any(not isinstance(key, str) for key in payload):
        raise PolicyError('a string-keyed native configuration is required')
    return digest({key: value for key, value in payload.items()
                   if key not in CONTROL_FIELDS and not _identity_neutral(key, value)})


def source_records(root: Path, files: tuple[Path, ...] | list[Path]) -> list[tuple[str, str]]:
    from . import source_identity as identity

    root = Path(root)
    if not root.is_absolute() or '..' in root.parts or any(p.is_symlink() for p in (root, *root.parents)) or not root.is_dir():
        raise PolicyError('implementation source owner must be an absolute non-symlink directory')
    if not files or len(files) > identity.MAX_FILES:
        raise PolicyError('bounded nonempty implementation source list required')
    entries, seen, total, components = [], set(), 0, False
    for item in files:
        path = Path(item)
        if not path.is_relative_to(root) or '..' in path.parts:
            raise PolicyError('implementation source is outside its owner')
        relative = path.relative_to(root).as_posix()
        if relative in seen or relative.startswith('@'):
            raise PolicyError('duplicate or reserved implementation source')
        seen.add(relative)
        data = identity.checked_source(path)
        components = components or identity.depends_on_components(data)
        total += len(data)
        entries.append((relative, sha256(data).hexdigest()))
    if components:
        component_root = identity.component_root()
        for path in identity.component_source_files(component_root):
            data = identity.checked_source(path)
            total += len(data)
            entries.append((identity.NAMESPACE + path.relative_to(component_root).as_posix(), sha256(data).hexdigest()))
    if total > identity.MAX_TOTAL_BYTES or len(entries) > identity.MAX_FILES:
        raise PolicyError('implementation source exceeds the admitted size')
    return sorted(entries)


def source_digest(root: Path, files: tuple[Path, ...] | list[Path]) -> str:
    return digest(source_records(root, files))


class RecipeBinding(PolicyModel):
    schema_version: Literal[1] = 1
    recipe_id: str = Field(pattern=r'^[A-Za-z0-9_-]{1,32}$')
    recipe_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    source_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    configuration_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')

    @field_validator('schema_version', mode='before')
    @classmethod
    def strict_version(cls, value):
        if type(value) is not int:
            raise ValueError('schema version must be an integer')
        return value


def verify_binding(binding: RecipeBinding, payload: Mapping, *, source_sha256: str) -> None:
    binding = RecipeBinding.model_validate(binding.model_dump(mode='json'))
    if binding.source_sha256 != source_sha256:
        raise PolicyError('recipe implementation source changed; explicit revision required')
    if binding.configuration_sha256 != configuration_digest(payload):
        raise PolicyError('recipe behavioural configuration changed; explicit revision required')
