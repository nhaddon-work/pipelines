{% macro replica_view(schema, x) %}
    {%- set query %}
        CREATE OR REPLACE VIEW REPLICA.{{ schema }}.{{ x.upper() }} AS
        SELECT *
        FROM {{ ref(x) }}
    {% endset -%}
    {% do run_query(query) %}
{% endmacro %}
