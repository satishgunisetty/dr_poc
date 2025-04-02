"""Tests for the settings module."""

import pytest

from src.config.settings import Settings


def test_settings_initialization() -> None:
    """Test that Settings can be initialized with default values."""
    settings = Settings()
    assert settings is not None
    assert settings.app_name == "databricks-project"
    assert settings.environment == "development"


def test_settings_custom_values() -> None:
    """Test that Settings can be initialized with custom values."""
    settings = Settings(app_name="test-app", environment="production")
    assert settings.app_name == "test-app"
    assert settings.environment == "production"


def test_settings_validation() -> None:
    """Test that Settings validates input values."""
    with pytest.raises(ValueError):
        Settings(environment="invalid-env")  # type: ignore
