{% test audit__consistency(model, column_name, regex, case_sensitive=false) %}
    SELECT * FROM {{ model }}

    {% if case_sensitive %}
        {% set operator = '!~' %}
    {% else %}
        {% set operator = '!~*' %}
    {% endif %}
    
    WHERE {{ column_name }} {{operator}} '{{ regex }}'

{% endtest %}