"""Shared test fixtures. One Spark session for the whole test run (starting Spark is slow)."""

import pytest

from sales_insights.common.config import load_config


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: starts a local Spark session (slow). Quick run: pytest -m 'not spark'")


def pytest_collection_modifyitems(items):
    """Every test that needs Spark (directly or through another fixture) is marked `spark` automatically."""
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def cfg():
    return load_config("local")


@pytest.fixture(scope="session")
def spark(cfg, tmp_path_factory):
    from sales_insights.common.spark import get_spark

    session = get_spark(cfg, app_name="sales-insights-tests")
    yield session
    session.stop()
