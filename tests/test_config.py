# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Test suite for validating the Epistemic SEER Configuration Policy."""

import os
import uuid
from collections.abc import Generator

import pytest
from pydantic import ValidationError

from coreason_etl_seer.config import EpistemicSeerConfigurationPolicy


@pytest.fixture(autouse=True)
def clean_env_state() -> Generator[None]:
    """Ensures a pristine environment variable state for each test execution."""
    original_api_key = os.environ.get("SEER_API_KEY")
    original_base_url = os.environ.get("SEER_BASE_URL")

    original_max_nesting = os.environ.get("MAX_TABLE_NESTING")
    original_namespace_uuid = os.environ.get("SEER_NAMESPACE_UUID")

    if "SEER_API_KEY" in os.environ:
        del os.environ["SEER_API_KEY"]
    if "SEER_BASE_URL" in os.environ:
        del os.environ["SEER_BASE_URL"]
    if "MAX_TABLE_NESTING" in os.environ:
        del os.environ["MAX_TABLE_NESTING"]
    if "SEER_NAMESPACE_UUID" in os.environ:
        del os.environ["SEER_NAMESPACE_UUID"]

    yield

    if original_api_key is not None:
        os.environ["SEER_API_KEY"] = original_api_key
    if original_base_url is not None:
        os.environ["SEER_BASE_URL"] = original_base_url
    if original_max_nesting is not None:
        os.environ["MAX_TABLE_NESTING"] = original_max_nesting
    if original_namespace_uuid is not None:
        os.environ["SEER_NAMESPACE_UUID"] = original_namespace_uuid


def test_configuration_initialization_success() -> None:
    """Verifies that the configuration policy instantiates correctly when the required API key is present."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"

    config = EpistemicSeerConfigurationPolicy()

    assert config.seer_api_key.get_secret_value() == "test-secret-key-123"
    assert str(config.seer_base_url) == "https://api.seer.cancer.gov/rest/"
    assert config.max_table_nesting == 0
    assert config.seer_namespace_uuid == uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def test_configuration_override_max_table_nesting() -> None:
    """Verifies that the max_table_nesting can be overridden via environment variables."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    os.environ["MAX_TABLE_NESTING"] = "1"

    config = EpistemicSeerConfigurationPolicy()

    assert config.max_table_nesting == 1


def test_configuration_invalid_max_table_nesting_format() -> None:
    """Ensures that invalid integers are rejected by the configuration policy."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    os.environ["MAX_TABLE_NESTING"] = "not-an-int"

    with pytest.raises(ValidationError) as exc_info:
        EpistemicSeerConfigurationPolicy()

    assert "max_table_nesting" in str(exc_info.value)
    assert "Input should be a valid integer" in str(exc_info.value)


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


def test_configuration_override_seer_namespace_uuid() -> None:
    """Verifies that the SEER namespace UUID can be overridden via environment variables."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    custom_uuid = "12345678-1234-5678-1234-567812345678"
    os.environ["SEER_NAMESPACE_UUID"] = custom_uuid

    config = EpistemicSeerConfigurationPolicy()

    assert config.seer_namespace_uuid == uuid.UUID(custom_uuid)


def test_configuration_invalid_seer_namespace_uuid() -> None:
    """Ensures that invalid UUID strings are rejected by the configuration policy."""
    os.environ["SEER_API_KEY"] = "test-secret-key-123"
    os.environ["SEER_NAMESPACE_UUID"] = "invalid-uuid-string"

    with pytest.raises(ValidationError) as exc_info:
        EpistemicSeerConfigurationPolicy()

    assert "seer_namespace_uuid" in str(exc_info.value)
    assert "Input should be a valid UUID" in str(exc_info.value)
