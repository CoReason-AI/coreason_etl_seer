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

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses

from coreason_etl_seer.client import EpistemicSeerClientPolicy
from coreason_etl_seer.config import EpistemicSeerConfigurationPolicy
from coreason_etl_seer.exceptions import (
    EpistemicSeerFaultEvent,
    SeerAuthenticationFaultEvent,
    SeerGatewayFaultEvent,
    SeerRateLimitFaultEvent,
    SeerResourceNotFoundFaultEvent,
)


@pytest.fixture
def test_config(monkeypatch: pytest.MonkeyPatch) -> EpistemicSeerConfigurationPolicy:
    """Provides a valid EpistemicSeerConfigurationPolicy for testing."""
    monkeypatch.setenv("SEER_API_KEY", "mock-api-key")
    monkeypatch.setenv("SEER_BASE_URL", "https://api.seer.cancer.gov/rest")
    monkeypatch.delenv("MAX_TABLE_NESTING", raising=False)
    monkeypatch.delenv("SEER_NAMESPACE_UUID", raising=False)
    return EpistemicSeerConfigurationPolicy()


@pytest.fixture
def seer_client(test_config: EpistemicSeerConfigurationPolicy) -> EpistemicSeerClientPolicy:
    """Provides an instance of EpistemicSeerClientPolicy for testing."""
    return EpistemicSeerClientPolicy(test_config)


@pytest.fixture(autouse=True)
def mock_time_sleep() -> Generator[MagicMock]:
    """Mocks time.sleep to avoid slowing down tests."""
    with patch("time.sleep") as mock_sleep:
        yield mock_sleep


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
def test_fetch_endpoint_manifold_http_error_404(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises a SeerResourceNotFoundFaultEvent on 404."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=404,
        json={"error": "Not Found"},
    )

    with pytest.raises(SeerResourceNotFoundFaultEvent):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_http_error_401(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises a SeerAuthenticationFaultEvent on 401."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=401,
        json={"error": "Unauthorized"},
    )

    with pytest.raises(SeerAuthenticationFaultEvent):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_http_error_429(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises a SeerRateLimitFaultEvent on 429 when retry fails."""
    url = "https://api.seer.cancer.gov/rest/disease"

    # Make the retry mechanism exhaust or fail
    seer_client.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))

    responses.add(
        responses.GET,
        url,
        status=429,
        json={"error": "Too Many Requests"},
    )

    with pytest.raises(SeerRateLimitFaultEvent):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_http_error_500(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises a SeerGatewayFaultEvent on 500 when retry fails."""
    url = "https://api.seer.cancer.gov/rest/disease"

    seer_client.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=0))

    responses.add(
        responses.GET,
        url,
        status=500,
        json={"error": "Internal Server Error"},
    )

    with pytest.raises(SeerGatewayFaultEvent):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_http_error_generic(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client correctly raises a EpistemicSeerFaultEvent on generic errors."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=418,
        json={"error": "I'm a teapot"},
    )

    with pytest.raises(EpistemicSeerFaultEvent):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_invalid_json(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client raises an EpistemicSeerFaultEvent if the response is not a JSON dictionary."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=200,
        json=["list", "instead", "of", "dict"],
    )

    with pytest.raises(EpistemicSeerFaultEvent, match="Expected JSON response to be a dictionary"):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_request_exception(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client handles requests.exceptions.RequestException."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        body=requests.exceptions.ConnectionError("Connection error"),
    )

    with pytest.raises(EpistemicSeerFaultEvent, match="Request Error: Connection error"):
        seer_client.fetch_endpoint_manifold("disease")


@responses.activate
def test_fetch_endpoint_manifold_value_error(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the client handles JSON parsing ValueError."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        status=200,
        body="not json",
    )

    with pytest.raises(EpistemicSeerFaultEvent, match="JSON Parsing Error"):
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


@responses.activate
def test_paginate_endpoint_manifold(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that the pagination logic correctly yields chunks of data."""
    url = "https://api.seer.cancer.gov/rest/disease"

    # First page
    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "1"}, {"id": "2"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )
    # Second page
    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "3"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "2", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))

    assert len(pages) == 2
    assert pages[0] == [{"id": "1"}, {"id": "2"}]
    assert pages[1] == [{"id": "3"}]


@responses.activate
def test_paginate_endpoint_manifold_missing_key(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies pagination logic handles empty/missing valid key in response."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        json={"something_else": [{"id": "1"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))
    assert len(pages) == 0


@responses.activate
def test_paginate_endpoint_manifold_staging_key(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies pagination logic handles 'staging' key in response."""
    url = "https://api.seer.cancer.gov/rest/staging"

    responses.add(
        responses.GET,
        url,
        json={"staging": [{"id": "1"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("staging", page_size=2))
    assert len(pages) == 1
    assert pages[0] == [{"id": "1"}]


@responses.activate
def test_paginate_endpoint_manifold_results_key(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies pagination logic handles 'results' fallback key in response."""
    url = "https://api.seer.cancer.gov/rest/unknown"

    responses.add(
        responses.GET,
        url,
        json={"results": [{"id": "99"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("unknown", page_size=2))
    assert len(pages) == 1
    assert pages[0] == [{"id": "99"}]


@responses.activate
def test_paginate_endpoint_manifold_exact_boundary(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies pagination when the page size perfectly matches the total records."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "1"}, {"id": "2"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )
    # The second page should return empty, terminating the loop.
    responses.add(
        responses.GET,
        url,
        json={"diseases": []},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "2", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))
    assert len(pages) == 1
    assert pages[0] == [{"id": "1"}, {"id": "2"}]


@responses.activate
def test_paginate_endpoint_manifold_with_params(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that custom query parameters are passed through alongside offset/count."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "1"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2", "type": "lung"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", params={"type": "lung"}, page_size=2))
    assert len(pages) == 1
    assert pages[0] == [{"id": "1"}]


@responses.activate
def test_paginate_endpoint_manifold_overrides_initial_pagination_params(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that if the user passes 'offset' or 'count' in params, the paginator strictly overrides them."""
    url = "https://api.seer.cancer.gov/rest/disease"

    # User attempts to pass offset=999, count=5
    # The paginator should ignore them and start at offset=0, count=2 (page_size)
    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "1"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2", "type": "lung"})],
    )

    initial_params = {"offset": 999, "count": 5, "type": "lung"}
    pages = list(seer_client.paginate_endpoint_manifold("disease", params=initial_params, page_size=2))
    assert len(pages) == 1
    assert pages[0] == [{"id": "1"}]


@responses.activate
def test_paginate_endpoint_manifold_handles_null_key(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that if the API returns explicit null for the key, it exits gracefully."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        json={"diseases": None},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))
    assert len(pages) == 0


@responses.activate
def test_paginate_endpoint_manifold_handles_empty_dict(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies that if the API returns an empty dict, it exits gracefully."""
    url = "https://api.seer.cancer.gov/rest/disease"

    responses.add(
        responses.GET,
        url,
        json={},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))
    assert len(pages) == 0


@responses.activate
def test_paginate_endpoint_manifold_midway_failure_recovery(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies pagination logic can recover from a transient failure (e.g. 500) midway through pages."""
    url = "https://api.seer.cancer.gov/rest/disease"

    # First page succeeds
    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "1"}, {"id": "2"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "0", "count": "2"})],
    )

    # Second page fails with 500, but requests Session Retry will try again.
    responses.add(
        responses.GET,
        url,
        json={"error": "Internal Server Error"},
        status=500,
        match=[responses.matchers.query_param_matcher({"offset": "2", "count": "2"})],
    )
    # Second page succeeds on retry
    responses.add(
        responses.GET,
        url,
        json={"diseases": [{"id": "3"}]},
        status=200,
        match=[responses.matchers.query_param_matcher({"offset": "2", "count": "2"})],
    )

    pages = list(seer_client.paginate_endpoint_manifold("disease", page_size=2))

    assert len(pages) == 2
    assert pages[0] == [{"id": "1"}, {"id": "2"}]
    assert pages[1] == [{"id": "3"}]


def test_proactive_rate_limit(seer_client: EpistemicSeerClientPolicy, mock_time_sleep: MagicMock) -> None:
    """Verifies that proactive rate limiting enforces the delay."""
    seer_client.last_request_time = 0.0  # Force a long time ago
    with patch("time.time", side_effect=[1.0, 1.5]):
        # The first time.time() call returns 1.0 (so elapsed = 1.0 - 0.0 = 1.0)
        # 1.0 is not < 1.0, so no sleep.
        seer_client._enforce_proactive_rate_limit()
        mock_time_sleep.assert_not_called()

    with patch("time.time", side_effect=[1.0, 1.5]):
        # Now set last_request_time to 0.5
        seer_client.last_request_time = 0.5
        # elapsed = 1.0 - 0.5 = 0.5. sleep_time = 1.0 - 0.5 = 0.5
        seer_client._enforce_proactive_rate_limit()
        mock_time_sleep.assert_called_once_with(0.5)
