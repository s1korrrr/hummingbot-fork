import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from hummingbot.model import get_declarative_base
from hummingbot.model.metadata import Metadata


@pytest.fixture
def native(tmp_path):
    get_declarative_base()
    engine = create_engine('sqlite:///' + str(tmp_path / 'native.sqlite'), connect_args={'timeout': 5})
    Metadata.__table__.create(engine)
    factory = sessionmaker(bind=engine)
    yield factory
    engine.dispose()
