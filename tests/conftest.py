"""Shared test fixtures for our-db."""

import pytest

from our_db.config import clear_config_cache


@pytest.fixture(autouse=True)
def _reset_config():
    """Reset global config between tests."""
    clear_config_cache()
    yield
    clear_config_cache()
