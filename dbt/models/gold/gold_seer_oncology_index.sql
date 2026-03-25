{{ config(
    materialized='table',
    alias='coreason_etl_seer_gold_seer_oncology_index'
) }}

select
    disease_id,
    coreason_id,
    disease_name,
    icdo3_site_code,
    icdo3_histology_code,
    behavior_code,
    concat_ws('-', icdo3_site_code, icdo3_histology_code, behavior_code) as full_icdo3_code
from {{ ref('SEER_Disease_Ontology') }}
where icdo3_site_code is not null
  and icdo3_histology_code is not null
  and behavior_code is not null
