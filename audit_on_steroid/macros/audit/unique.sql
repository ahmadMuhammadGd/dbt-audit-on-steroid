{% test audit__unique(model, column_name) %}
    with duplicates as (
        select {{ column_name }}
        from {{ model }}
        group by {{ column_name }}
        having count(*) > 1
    )

    select *
    from {{ model }}
    where {{ column_name }} in (select {{ column_name }} from duplicates)
{% endtest %}