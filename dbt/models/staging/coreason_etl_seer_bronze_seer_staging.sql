{{ config(materialized='table') }}

with raw_data as (
    select
        disease_id,
        coreason_id,
        api_version,
        raw_data
    from {{ source('bronze', 'seer_staging_raw') }}
)

select
    disease_id,
    coreason_id,
    raw_data->>'name' as staging_name,
    raw_data->>'schema' as staging_schema
from raw_data
