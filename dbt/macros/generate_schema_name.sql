{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {#
          Return ONLY the custom_schema_name instead of appending it to target_schema
          to enforce explicit schema names: "bronze", "silver", "gold".
        #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
