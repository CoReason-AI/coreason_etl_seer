{{ config(
    materialized='table',
    alias='coreason_etl_seer_gold_seer_staging_schemas'
) }}

select
    s.disease_id,
    s.coreason_id,
    s.staging_name,
    s.staging_schema,
    d.disease_name
from {{ ref('coreason_etl_seer_bronze_seer_staging') }} as s
left join {{ ref('SEER_Disease_Ontology') }} as d on s.disease_id = d.disease_id
