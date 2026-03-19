# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Defines the HTTP client policy for NCI SEER API interactions."""

import time
from collections.abc import Iterator
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import EpistemicSeerConfigurationPolicy
from .exceptions import (
    EpistemicSeerFaultEvent,
    SeerAuthenticationFaultEvent,
    SeerGatewayFaultEvent,
    SeerRateLimitFaultEvent,
    SeerResourceNotFoundFaultEvent,
)
from .utils.logger import logger


class EpistemicSeerClientPolicy:
    """Manages robust, rate-limited HTTP interactions with the SEER REST API."""

    def __init__(self, config: EpistemicSeerConfigurationPolicy) -> None:
        """Initializes the SEER client policy with the given configuration."""
        self.config = config
        self.session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        # Set default headers
        self.session.headers.update(
            {"X-SEERAPI-Key": self.config.seer_api_key.get_secret_value(), "Accept": "application/json"}
        )

        self.last_request_time: float = 0.0
        self.rate_limit_delay_seconds: float = 1.0  # Proactive 1s delay

    def _enforce_proactive_rate_limit(self) -> None:
        """Enforces a proactive delay to prevent 429 errors from the SEER API."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.rate_limit_delay_seconds:
            sleep_time = self.rate_limit_delay_seconds - elapsed
            logger.debug(f"Proactive rate limiting: sleeping for {sleep_time:.2f} seconds.")
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def fetch_endpoint_manifold(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetches data from a given SEER API endpoint."""
        base_url = str(self.config.seer_base_url).rstrip("/")
        url = f"{base_url}/{endpoint.lstrip('/')}"

        self._enforce_proactive_rate_limit()

        logger.debug(f"Initiating GET request to SEER API endpoint: {url}")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            try:
                data = response.json()
            except ValueError as val_err:
                logger.exception(f"Invalid JSON response from SEER API endpoint {url}")
                raise EpistemicSeerFaultEvent(f"JSON Parsing Error: {val_err}") from val_err

            if not isinstance(data, dict):
                raise EpistemicSeerFaultEvent("Expected JSON response to be a dictionary")
            return data

        except requests.exceptions.HTTPError as http_err:
            status_code = response.status_code
            logger.exception(f"HTTPError {status_code} from SEER API endpoint {url}")
            if status_code == 401 or status_code == 403:
                raise SeerAuthenticationFaultEvent(f"Authentication failed: {http_err}") from http_err
            if status_code == 404:
                raise SeerResourceNotFoundFaultEvent(f"Resource not found: {http_err}") from http_err
            if status_code == 429:
                raise SeerRateLimitFaultEvent(f"Rate limit exceeded: {http_err}") from http_err
            if status_code >= 500:
                raise SeerGatewayFaultEvent(f"Server error: {http_err}") from http_err
            raise EpistemicSeerFaultEvent(f"HTTP Error: {http_err}") from http_err
        except requests.exceptions.RequestException as req_err:
            logger.exception(f"Failed to fetch data from SEER API endpoint {url}")
            raise EpistemicSeerFaultEvent(f"Request Error: {req_err}") from req_err

    def paginate_endpoint_manifold(
        self, endpoint: str, params: dict[str, Any] | None = None, page_size: int = 100
    ) -> Iterator[list[dict[str, Any]]]:
        """Automatically paginates an endpoint, yielding chunks of records until exhaustion."""
        current_offset = 0
        if params is None:
            params = {}

        while True:
            current_params = params.copy()
            current_params["offset"] = current_offset
            current_params["count"] = page_size

            data = self.fetch_endpoint_manifold(endpoint, params=current_params)

            # Different endpoints might return lists under different keys.
            records = data.get("diseases", []) if "diseases" in data else data.get("results", [])
            if not records and "staging" in data:
                records = data["staging"]

            if not records:
                break

            yield records

            if len(records) < page_size:
                # We received fewer records than requested, so we're at the end.
                break

            current_offset += page_size
