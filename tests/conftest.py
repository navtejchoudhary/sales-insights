"""Shared test fixtures. One Spark session for the whole test run (starting Spark is slow)."""

import pytest

from sales_insights.common.config import load_config


@pytest.fixture(scope="session")
def cfg():
    return load_config("local")


@pytest.fixture(scope="session")
def spark(cfg, tmp_path_factory):
    from sales_insights.common.spark import get_spark

    session = get_spark(cfg, app_name="sales-insights-tests")
    yield session
    session.stop()
