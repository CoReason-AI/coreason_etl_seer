# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Defines the configuration policies for the Epistemic SEER integration."""

import uuid

from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class EpistemicSeerConfigurationPolicy(BaseSettings):
    """Configuration boundary for authenticating and routing to the NCI SEER API."""

    seer_api_key: SecretStr = Field(
        ...,
        description="The secret token required for X-SEERAPI-Key authentication header.",
    )

    seer_base_url: HttpUrl = Field(
        default=HttpUrl("https://api.seer.cancer.gov/rest/"),
        description="The base uniform resource locator for the SEER REST API.",
    )

    max_table_nesting: int = Field(
        default=0,
        description=(
            "Globally disables relational child-table creation in dlt, "
            "forcing deep ontology dictionary to land safely as a single JSONB column."
        ),
    )

    seer_namespace_uuid: uuid.UUID = Field(
        default=uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
        description="The deterministic UUID namespace for pre-computing coreason_id UUIDv5 hashes.",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
