{% set wap_model = this.identifier ~ '_wap' %}
SELECT *
FROM {{ ref(wap_model) }}
