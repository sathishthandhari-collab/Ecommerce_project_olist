{% test expect_column_pair_values_A_to_be_smaller_than_B(model, column_A, column_B, or_equal=false) %}
    

SELECT 
    {{ column_A }},
    {{ column_B }},
    '{{ column_A }} should be {{ "less than or equal to" if or_equal else "less than" }} {{ column_B }}' AS failure_reason
FROM {{ model }}
WHERE 
    {{ column_A }} IS NOT NULL 
    AND {{ column_B }} IS NOT NULL
    AND {{ column_A }} {{ ">=" if not or_equal else ">" }} {{ column_B }}

{% endtest %}
