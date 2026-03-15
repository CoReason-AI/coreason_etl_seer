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

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import EpistemicSeerConfigurationPolicy
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

    def fetch_endpoint_manifold(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Fetches data from a given SEER API endpoint."""
        base_url = str(self.config.seer_base_url).rstrip("/")
        url = f"{base_url}/{endpoint.lstrip('/')}"

        logger.debug(f"Initiating GET request to SEER API endpoint: {url}")

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not isinstance(data, dict):
                raise ValueError("Expected JSON response to be a dictionary")
            return data

        except requests.exceptions.RequestException:
            logger.exception(f"Failed to fetch data from SEER API endpoint {url}")
            raise
        except ValueError:
            logger.exception(f"Invalid JSON response from SEER API endpoint {url}")
            raise
