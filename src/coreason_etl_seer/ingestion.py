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


def _fetch_and_prepare_records(
    client: EpistemicSeerClientPolicy,
    namespace: uuid.UUID,
    endpoint: str,
    record_keys: list[str],
    id_alias: str,
    api_version: str = "latest",
) -> Iterator[list[dict[str, Any]]]:
    """Generic function to fetch data from SEER API, convert to Polars, and pre-compute coreason_id UUIDs."""
    logger.info(f"Fetching {endpoint} records from SEER API.")
    data = client.fetch_endpoint_manifold(endpoint)

    records = []
    for key in record_keys:
        if key in data:
            records = data[key]
            break

    if not records and "id" in data:
        records = [data]

    if not records:
        logger.warning(f"No {endpoint} records found in the payload.")
        yield []
        return

    df = pl.DataFrame(records)

    if "id" not in df.columns:
        logger.warning(f"No 'id' field found in {endpoint} records.")
        yield df.to_dicts()
        return

    df = df.with_columns(
        [
            pl.col("id").alias(id_alias),
            pl.col("id")
            .map_elements(lambda x: _generate_coreason_id(namespace, str(x)), return_dtype=pl.Utf8)
            .alias("coreason_id"),
            pl.lit(api_version).alias("api_version"),
        ]
    )

    yield df.to_dicts()


def fetch_and_prepare_disease_records(
    client: EpistemicSeerClientPolicy,
    namespace: uuid.UUID,
    api_version: str = "latest",
) -> Iterator[list[dict[str, Any]]]:
    """Fetches the /disease endpoint and returns mapped records in batches."""
    yield from _fetch_and_prepare_records(
        client=client,
        namespace=namespace,
        endpoint="disease",
        record_keys=["diseases", "results"],
        id_alias="disease_id",
        api_version=api_version,
    )


def fetch_and_prepare_staging_records(
    client: EpistemicSeerClientPolicy,
    namespace: uuid.UUID,
    api_version: str = "latest",
) -> Iterator[list[dict[str, Any]]]:
    """Fetches the /staging endpoint and returns mapped records in batches."""
    yield from _fetch_and_prepare_records(
        client=client,
        namespace=namespace,
        endpoint="staging",
        record_keys=["staging", "results"],
        id_alias="staging_id",
        api_version=api_version,
    )


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
