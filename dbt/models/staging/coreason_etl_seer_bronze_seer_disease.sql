{{ config(materialized='table') }}

with raw_data as (
    select
        disease_id,
        coreason_id,
        api_version,
        raw_data
    from {{ source('bronze', 'seer_disease_raw') }}
)

select
    disease_id,
    coreason_id,
    raw_data->>'name' as disease_name,
    raw_data->'icdo3'->>'site' as icdo3_site_code,
    raw_data->'icdo3'->>'histology' as icdo3_histology_code,
    raw_data->'icdo3'->>'behavior' as behavior_code,
    -- Extract parent_id for the adjacency list
    raw_data->>'parent_id' as parent_disease_id
from raw_data
