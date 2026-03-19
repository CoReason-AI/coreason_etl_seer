{{ config(materialized='table') }}

with recursive disease_hierarchy as (
    -- Anchor member
    select
        disease_id,
        parent_disease_id,
        coreason_id,
        disease_name,
        icdo3_site_code,
        icdo3_histology_code,
        behavior_code,
        1 as hierarchy_level,
        disease_id::text as lineage_path
    from {{ ref('coreason_etl_seer_bronze_seer_disease') }}
    where parent_disease_id is null

    union all

    -- Recursive member
    select
        child.disease_id,
        child.parent_disease_id,
        child.coreason_id,
        child.disease_name,
        child.icdo3_site_code,
        child.icdo3_histology_code,
        child.behavior_code,
        parent.hierarchy_level + 1 as hierarchy_level,
        parent.lineage_path || '->' || child.disease_id as lineage_path
    from {{ ref('coreason_etl_seer_bronze_seer_disease') }} as child
    inner join disease_hierarchy as parent on child.parent_disease_id = parent.disease_id
)

select
    disease_id,
    coreason_id,
    disease_name,
    icdo3_site_code,
    icdo3_histology_code,
    behavior_code,
    parent_disease_id,
    hierarchy_level,
    lineage_path
from disease_hierarchy
