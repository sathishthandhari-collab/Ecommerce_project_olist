{% test expect_foreign_key_relationships_with_context(model, column_name, to, field, context_columns=[]) %}
    

WITH source_data AS (
    SELECT 
        {{ column_name }},
        {% for col in context_columns %}
    {{ col }},
        {% endfor %}
        COUNT(*) AS occurrence_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
    GROUP BY 
        {{ column_name }}
{%- for col in context_columns -%}
    , {{ col }}
{%- endfor %}
),

target_data AS (
    SELECT DISTINCT {{ field }}
    FROM {{ to }}
),

orphaned_records AS (
    SELECT s.*
    FROM source_data s
    LEFT JOIN target_data t ON s.{{ column_name }} = t.{{ field }}
    WHERE t.{{ field }} IS NULL
)

SELECT 
    {{ column_name }},
    {% for col in context_columns %}
{{ col }},
    {% endfor %}
    occurrence_count,
    '{{ column_name }} not found in {{ to }}.{{ field }}' AS failure_reason
FROM orphaned_records

{% endtest %}
