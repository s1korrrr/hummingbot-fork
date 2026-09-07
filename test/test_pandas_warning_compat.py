from types import SimpleNamespace

import pandas as pd

from controllers.pandas_compat import pandas_copy_on_write_warning_category


class _Pandas4Warning(Warning):
    pass


def test_copy_on_write_warning_uses_pandas_3_category_when_available():
    errors = SimpleNamespace(Pandas4Warning=_Pandas4Warning)

    assert pandas_copy_on_write_warning_category(errors) is _Pandas4Warning


def test_copy_on_write_warning_falls_back_for_pandas_2():
    assert pandas_copy_on_write_warning_category(SimpleNamespace()) is FutureWarning


def test_copy_on_write_warning_matches_installed_pandas():
    expected = getattr(pd.errors, "Pandas4Warning", FutureWarning)

    assert pandas_copy_on_write_warning_category(pd.errors) is expected
