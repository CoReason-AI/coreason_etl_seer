{{ config(materialized='table') }}

select
    disease_id,
    coreason_id,
    disease_name,
    icdo3_site_code,
    icdo3_histology_code,
    behavior_code,
    concat_ws('-', icdo3_site_code, icdo3_histology_code, behavior_code) as full_icdo3_code
from {{ ref('coreason_etl_seer_silver_seer_disease_ontology') }}
where icdo3_site_code is not null
  and icdo3_histology_code is not null
  and behavior_code is not null
