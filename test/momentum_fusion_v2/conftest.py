import pytest

from .fixtures import recipe


@pytest.fixture
def r():
    return recipe()
