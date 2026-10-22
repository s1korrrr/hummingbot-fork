import pytest

from .fixtures import recipe


@pytest.mark.parametrize('variant', ['C2', 'C3', 'all'])
def test_optional_policies_cannot_replace_canonical_b0_c1(variant):
    with pytest.raises(ValueError):
        recipe(variant)
