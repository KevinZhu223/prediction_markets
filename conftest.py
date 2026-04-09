import pytest

from config import Config


@pytest.fixture
def has_key() -> bool:
    """Fixture used by integration-style tests that require Kalshi credentials."""
    return bool(Config.KALSHI_API_KEY)
