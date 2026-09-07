"""Compatibility helpers for warning categories that differ by pandas major version."""


def pandas_copy_on_write_warning_category(errors_module: object) -> type[Warning]:
    return getattr(errors_module, "Pandas4Warning", FutureWarning)
