-- This complex test asserts that the 'full_icdo3_code' in the gold index
-- is formed by exactly concatenating icdo3_site_code, icdo3_histology_code,
-- and behavior_code, each non-empty, separated by hyphens.
-- It specifically catches edge cases where concatenated parts might be null, empty, or incorrectly formatted.

with validation as (
    select
        disease_id,
        full_icdo3_code,
        icdo3_site_code,
        icdo3_histology_code,
        behavior_code
    from {{ ref('gold_seer_oncology_index') }}
),

validation_errors as (
    select
        disease_id,
        full_icdo3_code
    from validation
    where
        -- The concatenated field should not be null
        full_icdo3_code is null
        -- The concatenated field should equal the three components joined by '-'
        or full_icdo3_code != concat_ws('-', icdo3_site_code, icdo3_histology_code, behavior_code)
        -- The components themselves must not be null or empty
        or icdo3_site_code is null or trim(icdo3_site_code) = ''
        or icdo3_histology_code is null or trim(icdo3_histology_code) = ''
        or behavior_code is null or trim(behavior_code) = ''
        -- Ensure the pattern roughly looks like CXX.X-XXXX-X
        or full_icdo3_code !~ '^C\d{2}\.\d-\d{4}-\d$'
)

select *
from validation_errors
