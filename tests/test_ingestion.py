# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Test suite for validating the Ingestion Strategy."""

import uuid

import pytest
import responses

from coreason_etl_seer.client import EpistemicSeerClientPolicy
from coreason_etl_seer.config import EpistemicSeerConfigurationPolicy
from coreason_etl_seer.ingestion import (
    _generate_coreason_id,
    fetch_and_prepare_disease_records,
    fetch_and_prepare_staging_records,
    seer_source,
)


@pytest.fixture
def test_config(monkeypatch: pytest.MonkeyPatch) -> EpistemicSeerConfigurationPolicy:
    """Provides a valid config."""
    monkeypatch.setenv("SEER_API_KEY", "mock-api-key")
    monkeypatch.setenv("SEER_BASE_URL", "https://api.seer.cancer.gov/rest/")
    monkeypatch.delenv("MAX_TABLE_NESTING", raising=False)
    monkeypatch.delenv("SEER_NAMESPACE_UUID", raising=False)
    return EpistemicSeerConfigurationPolicy()


@pytest.fixture
def seer_client(test_config: EpistemicSeerConfigurationPolicy) -> EpistemicSeerClientPolicy:
    """Provides an instance of EpistemicSeerClientPolicy for testing."""
    return EpistemicSeerClientPolicy(test_config)


def test_generate_coreason_id() -> None:
    """Verifies deterministic UUID generation."""
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    val = _generate_coreason_id(ns, "123")
    assert isinstance(val, str)
    # the same inputs must produce the same outputs
    assert val == _generate_coreason_id(ns, "123")


@responses.activate
def test_fetch_and_prepare_disease_records(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies fetching and preparing disease records."""
    url = "https://api.seer.cancer.gov/rest/disease"
    mock_data = {
        "diseases": [
            {"id": "123", "name": "Lung Cancer", "nested": {"val": 1}},
            {"id": "456", "name": "Breast Cancer", "nested": {"val": 2}},
        ]
    }

    responses.add(
        responses.GET,
        url,
        json=mock_data,
        status=200,
    )

    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    batches = list(fetch_and_prepare_disease_records(seer_client, ns))

    assert len(batches) == 1
    batch = batches[0]
    assert len(batch) == 2

    assert "disease_id" in batch[0]
    assert batch[0]["disease_id"] == "123"
    assert "coreason_id" in batch[0]
    assert "api_version" in batch[0]
    assert batch[0]["name"] == "Lung Cancer"


@responses.activate
def test_seer_source(test_config: EpistemicSeerConfigurationPolicy) -> None:
    """Verifies DLT source generation and processing."""
    disease_url = "https://api.seer.cancer.gov/rest/disease"
    staging_url = "https://api.seer.cancer.gov/rest/staging"

    responses.add(
        responses.GET,
        disease_url,
        json={"diseases": [{"id": "123", "name": "Lung Cancer"}]},
        status=200,
    )

    responses.add(
        responses.GET,
        staging_url,
        json={"staging": [{"id": "789", "schema": "TNM"}]},
        status=200,
    )

    source = seer_source(test_config)
    assert len(source.resources) == 2

    # Evaluate disease resource
    d_res = source.resources["seer_disease_raw"]
    d_data = list(d_res)
    # d_data should be a list containing the batch.
    # In dlt, list(resource) yields items unless chunked.
    assert len(d_data) == 1
    assert d_data[0]["disease_id"] == "123"
    assert "coreason_id" in d_data[0]
    assert "api_version" in d_data[0]
    assert d_data[0]["raw_data"] == {"id": "123", "name": "Lung Cancer"}

    # Evaluate staging resource
    s_res = source.resources["seer_staging_raw"]
    s_data = list(s_res)
    assert len(s_data) == 1
    assert s_data[0]["disease_id"] == "789"
    assert "coreason_id" in s_data[0]
    assert "api_version" in s_data[0]
    assert s_data[0]["raw_data"] == {"id": "789", "schema": "TNM"}


@responses.activate
def test_fetch_and_prepare_disease_records_empty_and_fallback(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies fetching disease records with empty and fallback logic."""
    url = "https://api.seer.cancer.gov/rest/disease"
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

    # Empty payload
    responses.add(responses.GET, url, json={}, status=200)
    batches = list(fetch_and_prepare_disease_records(seer_client, ns))
    assert batches == [[]]
    responses.remove(responses.GET, url)

    # Single item with id but no diseases list
    responses.add(responses.GET, url, json={"id": "999", "name": "Test"}, status=200)
    batches = list(fetch_and_prepare_disease_records(seer_client, ns))
    assert len(batches) == 1
    assert batches[0][0]["disease_id"] == "999"
    responses.remove(responses.GET, url)

    # Missing id field
    responses.add(responses.GET, url, json={"diseases": [{"name": "No ID"}]}, status=200)
    batches = list(fetch_and_prepare_disease_records(seer_client, ns))
    assert len(batches) == 1
    assert "id" not in batches[0][0]
    responses.remove(responses.GET, url)


@responses.activate
def test_fetch_and_prepare_staging_records_empty_and_fallback(seer_client: EpistemicSeerClientPolicy) -> None:
    """Verifies fetching staging records with empty and fallback logic."""
    url = "https://api.seer.cancer.gov/rest/staging"
    ns = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

    # Empty payload
    responses.add(responses.GET, url, json={}, status=200)
    batches = list(fetch_and_prepare_staging_records(seer_client, ns))
    assert batches == [[]]
    responses.remove(responses.GET, url)

    # Single item with id but no list
    responses.add(responses.GET, url, json={"id": "888", "name": "Test Staging"}, status=200)
    batches = list(fetch_and_prepare_staging_records(seer_client, ns))
    assert len(batches) == 1
    assert batches[0][0]["staging_id"] == "888"
    responses.remove(responses.GET, url)

    # Missing id field
    responses.add(responses.GET, url, json={"staging": [{"name": "No ID staging"}]}, status=200)
    batches = list(fetch_and_prepare_staging_records(seer_client, ns))
    assert len(batches) == 1
    assert "id" not in batches[0][0]
    responses.remove(responses.GET, url)
