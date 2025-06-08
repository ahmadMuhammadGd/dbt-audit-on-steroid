{% macro build_batch_fragment(batch_id, test, batch_id_column) %}
    SELECT 
        '{{ batch_id }}'::TEXT as batch_id,
        '{{ test.model_id }}'::TEXT as model_id,
        '{{ test.model_relation }}'::TEXT as relation_name,
        '{{ test.column_name }}'::TEXT as column_name,
        '{{ test.test_name }}'::TEXT as kpi_name,
        ft.failed_count::INT as failed_rows,
        tt.total_count::INT as total_rows,
        
        CASE 
            WHEN tt.total_count = 0 THEN 0 
            ELSE ROUND((ft.failed_count * 100.0) / tt.total_count, 2)
        END as failure_pct,
        
        CASE 
            WHEN tt.total_count = 0 THEN 100 
            ELSE ROUND(100 - (ft.failed_count * 100.0) / tt.total_count, 2)
        END as success_pct,
        
        fs.failed_sample,
        CURRENT_TIMESTAMP as dbt_created_at
    FROM 
        {# Failed rows count for this specific batch #}
        (
            SELECT COUNT(*) as failed_count
            FROM {{ test.test_relation }}
            WHERE {{ batch_id_column }} = '{{ batch_id }}'
        ) ft,
        
        {# Total rows count for this batch from the model #}
        (
            SELECT COUNT(*) as total_count 
            FROM {{ test.model_relation }}
            WHERE {{ batch_id_column }} = '{{ batch_id }}'
        ) tt,
        
        {# Sample of failed records for this batch #}
        (
            SELECT JSON_AGG(sample_data) as failed_sample
            FROM (
                SELECT row_to_json(t) AS sample_data
                FROM {{ test.test_relation }} t
                WHERE t.{{ batch_id_column }} = '{{ batch_id }}'
                LIMIT 100
            ) sample
        ) AS fs
{% endmacro %}