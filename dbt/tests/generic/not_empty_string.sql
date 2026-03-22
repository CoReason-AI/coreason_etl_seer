{% test not_empty_string(model, column_name) %}

with validation as (
    select
        {{ column_name }} as string_field
    from {{ model }}
),

validation_errors as (
    select
        string_field
    from validation
    where string_field is not null
      and trim(string_field) = ''
)

select *
from validation_errors

{% endtest %}
