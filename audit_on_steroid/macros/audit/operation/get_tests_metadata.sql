{% macro __get_test_metadata() %}
    {% set test_metadata = [] %}
    
    {% if execute %}
        {% for test_node in graph.nodes.values() | selectattr("resource_type", "equalto", "test") %}
            {% set model_node = graph.nodes.get(test_node.attached_node) %}
            {% if model_node and 
                test_node.test_metadata.name and 
                test_node.test_metadata.name.startswith('audit_') %}
                {% do test_metadata.append({
                    'test_name': test_node.test_metadata.name,
                    'column_name': test_node.column_name | default(''),
                    'test_relation': test_node.relation_name,
                    'model_relation': model_node.relation_name,
                    'model_id': model_node.unique_id
                }) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    
    {{ return(test_metadata) }}
{% endmacro %}



