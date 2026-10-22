def pytest_configure(config):
    config.addinivalue_line('filterwarnings', 'ignore::ResourceWarning')
    config.addinivalue_line('filterwarnings', 'ignore::pytest.PytestUnraisableExceptionWarning')
