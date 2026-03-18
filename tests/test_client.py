# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Test suite for validating the Epistemic SEER HTTP Client Policy."""

import pytest
import requests
import responses

from coreason_etl_seer.client import EpistemicSeerClientPolicy
from coreason_etl_seer.config import EpistemicSeerConfigurationPolicy


@pytest.fixture
def test_config(monkeypatch: pytest.MonkeyPatch) -> EpistemicSeerConfigurationPolicy:
    """Provides a valid EpistemicSeerConfigurationPolicy for testing."""
    monkeypatch.setenv("SEER_API_KEY", "mock-api-key")
    monkeypatch.setenv("SEER_BASE_URL", "https://api.seer.cancer.gov/rest/")
    monkeypatch.delenv("MAX_TABLE_NESTING", raising=False)
    monkeypatch.delenv("SEER_NAMESPACE_UUID", raising=False)
    return EpistemicSeerConfigurationPolicy()


@pytest.fixture
def seer_client(test_config: EpistemicSeerConfigurationPolicy) -> EpistemicSeerClientPolicy:
    """Provides an instance of EpistemicSeerClientPolicy for testing."""
    return EpistemicSeerClientPolicy(test_config)


@responses.activate
def test_fetch_endpoint_manifold_success(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly fetches data from a given endpoint."""
    url = "https://api.seer.cancer.gov/rest/disease"
    mock_data = {"id": "123", "name": "Lung Cancer"}

    responses.add(
        responses.GET,
        url,
        json=mock_data,
        status=200,
        match=[
            responses.matchers.header_matcher({"X-SEERAPI-Key": "mock-api-key", "Accept": "application/json"}),
        ],
    )

    data = seer_client.fetch_endpoint_manifold("disease")

    assert data == mock_data


@responses.activate
def test_fetch_endpoint_manifold_retry_on_429(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly retries a request on a 429 status code."""
    url = "https://api.seer.cancer.gov/rest/disease"
    mock_data = {"id": "123", "name": "Lung Cancer"}

    responses.add(
        responses.GET,
        url,
        status=429,
        json={"error": "Too Many Requests"},
    )
    responses.add(
        responses.GET,
        url,
        status=200,
        json=mock_data,
    )

    data = seer_client.fetch_endpoint_manifold("disease")

    assert data == mock_data
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_endpoint_manifold_retry_on_500(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly retries a request on a 500 status code."""
    url = "https://api.seer.cancer.gov/rest/disease"
    mock_data = {"id": "123", "name": "Lung Cancer"}

    responses.add(
        responses.GET,
        url,
        status=500,
        json={"error": "Internal Server Error"},
    )
    responses.add(
        responses.GET,
        url,
        status=200,
        json=mock_data,
    )

    data = seer_client.fetch_endpoint_manifold("disease")

    assert data == mock_data
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_endpoint_manifold_http_error(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises an exception on HTTP errors."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=404,
        json={"error": "Not Found"},
    )

    with pytest.raises(requests.exceptions.HTTPError):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_invalid_json(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client raises a ValueError if the response is not a JSON dictionary."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=200,
        json=["list", "instead", "of", "dict"],
    )

    with pytest.raises(ValueError, match="Expected JSON response to be a dictionary"):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_with_params(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly sends query parameters."""
    url = "https://api.seer.cancer.gov/rest/disease"
    mock_data = {"id": "123", "name": "Lung Cancer"}

    responses.add(
        responses.GET,
        url,
        json=mock_data,
        status=200,
        match=[
            responses.matchers.query_param_matcher({"param1": "value1", "param2": "value2"}),
        ],
    )

    data = seer_client.fetch_endpoint_manifold("disease", params={"param1": "value1", "param2": "value2"})

    assert data == mock_data
