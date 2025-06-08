{% macro cast__str_to_numeric(column_name) %}
    CASE 
        WHEN TRIM({{ column_name }}::TEXT) ~ '^[0-9]+\.{,1}[0-9]*$' 
        THEN {{ column_name }}::REAL
        ELSE NULL
    END
{% endmacro %}