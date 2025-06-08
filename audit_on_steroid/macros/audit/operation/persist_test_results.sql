{% macro audit__persist_test_results(
    audit_schema='audit__reports', 
    audit_table_name='audit_report',
    batch_id_column='batch_id'
) %}
    
    {% do adapter.create_schema(
        api.Relation.create(database=target.database, schema=audit_schema)
    ) %}

    {% set audit_select_queries = [] %}
    {% set audit_ddl %}
        CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ audit_schema }}.{{ audit_table_name }} (
            batch_id            TEXT,
            model_id            TEXT,
            relation_name       TEXT,
            column_name         TEXT,
            kpi_name            TEXT,
            failed_rows         INT,
            total_rows          INT,
            failure_pct         NUMERIC(5, 2),
            success_pct         NUMERIC(5, 2),
            failed_sample       JSON,
            query_text          TEXT,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    {% endset %}

    {% do run_query(audit_ddl) %}

    {% set test_metadata = __get_test_metadata() %}
    {% set total_processed = 0 %}
    
    {% for test in test_metadata if test.test_relation and test.model_relation %}
        {# Get unprocessed batches for this specific test/model combination #}
        {% set unprocessed_batches_query %}
            SELECT DISTINCT tr.{{ batch_id_column }}
            FROM {{ test.test_relation }} tr
            WHERE NOT EXISTS (
                SELECT 1
                FROM {{ audit_schema }}.{{ audit_table_name }} ar
                WHERE ar.batch_id = tr.{{ batch_id_column }}::TEXT
                    AND ar.model_id = '{{ test.model_id }}'
                    AND ar.kpi_name = '{{ test.test_name }}'
                    AND ar.column_name = '{{ test.column_name }}'
            )
            ORDER BY tr.{{ batch_id_column }}
        {% endset %}
        
        {% set unprocessed_batches = run_query(unprocessed_batches_query).columns[0].values() %}
        
        {% do log('🫡 ['~ test.test_name ~'|' ~ test.column_name ~'] -> Found unprocessed baches: '~ unprocessed_batches, info=true) %}
        
        {% if unprocessed_batches %}
            {# Process each batch individually #}
            {% set batch_id = unprocessed_batches[0] %}
            
            {% set batch_sql_main = build_batch_fragment(batch_id, test, batch_id_column) %}

            {% set batch_sql %}
                SELECT *, ARRAY['{{ batch_sql_main | replace("'", "''") }}'] as query_text
                FROM ( {{ batch_sql_main }} ) AS __results
            {% endset %}
            
            {% do audit_select_queries.append(batch_sql) %}

        {% endif %}
    {% endfor %}
        
    {% set audit_insert_query %}
        INSERT INTO {{ target.database }}.{{ audit_schema }}.{{ audit_table_name }} (
            batch_id, 
            model_id, 
            relation_name, 
            column_name, 
            kpi_name, 
            failed_rows, 
            total_rows, 
            failure_pct, 
            success_pct, 
            failed_sample, 
            created_at,
            query_text
        )
        {{ audit_select_queries | join(' UNION ALL ') }}
    {% endset %}

    {% do run_query(audit_insert_query) %}
    
    {% do log(audit_insert_query) %}
    
    {% if audit_select_queries %}
        {% do log("🎉 Finished constructing the query to process " ~ audit_select_queries | length ~ " batch-test combinations", info=true) %}
    {% else %}
        {% do log("⚠️ No unprocessed batches found", info=true) %}
    {% endif %}
    
{% endmacro %}