# Copyright (c) 2026 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_seer

"""Defines the Ingestion Strategy for SEER API using Polars and dlt."""

import uuid
from collections.abc import Iterator
from typing import Any

import dlt
import polars as pl

from .client import EpistemicSeerClientPolicy
from .config import EpistemicSeerConfigurationPolicy
from .utils.logger import logger


def _generate_coreason_id(namespace: uuid.UUID, seer_id: str) -> str:
    """AGENT INSTRUCTION: Compute deterministic UUIDv5 hash for a given SEER ID."""
    return str(uuid.uuid5(namespace, seer_id))


def fetch_and_prepare_disease_records(
    client: EpistemicSeerClientPolicy,
    namespace: uuid.UUID,
    api_version: str = "latest",
) -> Iterator[list[dict[str, Any]]]:
    """Fetches the /disease endpoint and returns mapped records in batches."""
    logger.info("Fetching disease records from SEER API.")
    data = client.fetch_endpoint_manifold("disease")

    # Check if the response is a dictionary containing a list of diseases
    # The API structure usually nests items or is a flat list depending on endpoint.
    # We will assume a 'diseases' or a similar structure or maybe the endpoint returns a list directly?
    # Actually `fetch_endpoint_manifold` returns `dict[str, Any]`. We assume it has a key for records,
    # e.g., 'results' or similar, but the user says "Extract the id from the SEER API response".

    records = data.get("diseases", []) if "diseases" in data else data.get("results", [])
    if not records and "id" in data:
        records = [data]  # Fallback if single object

    if not records:
        logger.warning("No disease records found in the payload.")
        yield []
        return

    df = pl.DataFrame(records)

    # We need to compute coreason_id.
    if "id" not in df.columns:
        logger.warning("No 'id' field found in disease records.")
        yield df.to_dicts()
        return

    # Using map_batches with pre-computed coreason_ids
    df = df.with_columns(
        [
            pl.col("id").alias("disease_id"),
            pl.col("id")
            .map_elements(lambda x: _generate_coreason_id(namespace, str(x)), return_dtype=pl.Utf8)
            .alias("coreason_id"),
            pl.lit(api_version).alias("api_version"),
        ]
    )

    yield df.to_dicts()


def fetch_and_prepare_staging_records(
    client: EpistemicSeerClientPolicy,
    namespace: uuid.UUID,
    api_version: str = "latest",
) -> Iterator[list[dict[str, Any]]]:
    """Fetches the /staging endpoint and returns mapped records in batches."""
    logger.info("Fetching staging records from SEER API.")
    data = client.fetch_endpoint_manifold("staging")

    records = data.get("staging", []) if "staging" in data else data.get("results", [])
    if not records and "id" in data:
        records = [data]

    if not records:
        logger.warning("No staging records found in the payload.")
        yield []
        return

    df = pl.DataFrame(records)

    if "id" not in df.columns:
        logger.warning("No 'id' field found in staging records.")
        yield df.to_dicts()
        return

    df = df.with_columns(
        [
            pl.col("id").alias("staging_id"),  # Or keep as disease_id, but maybe staging_id is better
            pl.col("id")
            .map_elements(lambda x: _generate_coreason_id(namespace, str(x)), return_dtype=pl.Utf8)
            .alias("coreason_id"),
            pl.lit(api_version).alias("api_version"),
        ]
    )

    yield df.to_dicts()


@dlt.source
def seer_source(config: EpistemicSeerConfigurationPolicy) -> Any:
    """The DLT source for SEER API data."""
    client = EpistemicSeerClientPolicy(config)
    namespace = config.seer_namespace_uuid

    @dlt.resource(name="seer_disease_raw", write_disposition="merge", primary_key="disease_id", merge_key="disease_id")
    def disease_resource() -> Iterator[list[dict[str, Any]]]:
        for batch in fetch_and_prepare_disease_records(client, namespace):
            processed_batch = [
                {
                    "disease_id": row["disease_id"],
                    "coreason_id": row["coreason_id"],
                    "api_version": row["api_version"],
                    "raw_data": {k: v for k, v in row.items() if k not in ("disease_id", "coreason_id", "api_version")},
                }
                for row in batch
            ]
            yield processed_batch

    @dlt.resource(
        name="seer_staging_raw",
        write_disposition="merge",
        primary_key="disease_id",
        merge_key="disease_id",
    )
    def staging_resource() -> Iterator[list[dict[str, Any]]]:
        for batch in fetch_and_prepare_staging_records(client, namespace):
            processed_batch = [
                {
                    "disease_id": str(row.get("staging_id", row.get("id"))),
                    "coreason_id": row["coreason_id"],
                    "api_version": row["api_version"],
                    "raw_data": {
                        k: v
                        for k, v in row.items()
                        if k not in ("disease_id", "staging_id", "coreason_id", "api_version")
                    },
                }
                for row in batch
            ]
            yield processed_batch

    return [disease_resource, staging_resource]
