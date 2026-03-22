{% test valid_uuid(model, column_name) %}

with validation as (
    select
        {{ column_name }} as uuid_field
    from {{ model }}
),

validation_errors as (
    select
        uuid_field
    from validation
    where uuid_field is not null
      -- A standard UUID has 36 characters (including 4 hyphens) and matches this regex
      and uuid_field !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

select *
from validation_errors

{% endtest %}
