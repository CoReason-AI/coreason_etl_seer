# The Prosperity Public License 3.0.0
# Contributor: CoReason, Inc.
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Test suite for validating the Epistemic SEER Configuration Policy."""

import os
from collections.abc import Generator

import pytest
from pydantic import ValidationError

from coreason_etl_seer.config import EpistemicSeerConfigurationPolicy


@pytest.fixture(autouse=True)
def clean_env_state() -> Generator[None]:
    """Ensures a pristine environment variable state for each test execution."""
    original_api_key = os.environ.get("SEER_API_KEY")
    original_base_url = os.environ.get("SEER_BASE_URL")

    if "SEER_API_KEY" in os.environ:
        del os.environ["SEER_API_KEY"]
    if "SEER_BASE_URL" in os.environ:
        del os.environ["SEER_BASE_URL"]

    yield

    if original_api_key is not None:
        os.environ["SEER_API_KEY"] = original_api_key
    if original_base_url is not None:
        os.environ["SEER_BASE_URL"] = original_base_url


def test_configuration_initialization_success() -> None:
    """Verifies that the configuration policy instantiates correctly when the required API key is present."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"

    config = EpistemicSeerConfigurationPolicy()

    assert config.seer_api_key.get_secret_value() == "test-secret-key-123"
    assert str(config.seer_base_url) == "https://api.seer.cancer.gov/rest/"


def test_configuration_fails_without_api_key() -> None:
    """Ensures that the configuration policy raises a ValidationError if the required API key is omitted."""
    with pytest.raises(ValidationError) as exc_info:
        EpistemicSeerConfigurationPolicy()

    assert "seer_api_key" in str(exc_info.value)
    assert "Field required" in str(exc_info.value)


def test_configuration_override_base_url() -> None:
    """Verifies that the default base URL can be overridden via environment variables."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    os.environ["SEER_BASE_URL"] = "https://mock-api.cancer.gov/rest/"

    config = EpistemicSeerConfigurationPolicy()

    assert str(config.seer_base_url) == "https://mock-api.cancer.gov/rest/"


def test_configuration_invalid_base_url_format() -> None:
    """Ensures that invalid URL formats are rejected by the configuration policy."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    os.environ["SEER_BASE_URL"] = "not-a-valid-url"

    with pytest.raises(ValidationError) as exc_info:
        EpistemicSeerConfigurationPolicy()

    assert "seer_base_url" in str(exc_info.value)
    assert "Input should be a valid URL" in str(exc_info.value)


def test_configuration_secret_string_representation() -> None:
    """Confirms that the secret API key is not exposed when the model is converted to a string or dictionary."""
    os.environ["SEER_API_KEY"] = "super-secret-key-do-not-log"

    config = EpistemicSeerConfigurationPolicy()

    assert "super-secret-key-do-not-log" not in repr(config)
    assert "super-secret-key-do-not-log" not in str(config.model_dump())
