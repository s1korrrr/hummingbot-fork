from hummingbot.strategy_v2.market_context.market_picture import provenance


def test_picture_identity_changes_with_nested_calculation_source(monkeypatch, tmp_path):
    root = tmp_path.resolve() / 'context'
    nested = root / 'market_picture'
    nested.mkdir(parents=True)
    (root / 'features.py').write_text('VALUE = 1\n')
    calculation = nested / 'metrics.py'
    calculation.write_text('VALUE = 2\n')
    monkeypatch.setattr(provenance, 'package_root', lambda: root)
    before = provenance.package_source_digest()
    calculation.write_text('VALUE = 3\n')
    assert provenance.package_source_digest() != before


def test_picture_identity_includes_canonical_dependencies(monkeypatch, tmp_path):
    root = tmp_path.resolve() / 'context'
    (root / 'market_picture').mkdir(parents=True)
    source = root / 'features.py'
    source.write_text('VALUE = 1\n')
    (root / 'market_picture' / 'metrics.py').write_text('VALUE = 2\n')
    monkeypatch.setattr(provenance, 'package_root', lambda: root)
    before = provenance.package_source_digest()
    source.write_text('VALUE = 3\n')
    assert provenance.package_source_digest() != before
