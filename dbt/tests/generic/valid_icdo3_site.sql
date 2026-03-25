{% test valid_icdo3_site(model, column_name) %}

with validation as (
    select
        {{ column_name }} as icdo3_site_code
    from {{ model }}
),

validation_errors as (
    select
        icdo3_site_code
    from validation
    where icdo3_site_code is not null
      and icdo3_site_code != ''
      -- Standard ICD-O-3 primary site codes are typically C followed by 2 digits, a dot, and 1 digit (e.g. C34.9)
      -- Allow optionally without dot for weird cases, but strict format is usually preferred. Let's enforce strict CXX.X
      and icdo3_site_code !~ '^C\d{2}\.\d$'
)

select *
from validation_errors

{% endtest %}
