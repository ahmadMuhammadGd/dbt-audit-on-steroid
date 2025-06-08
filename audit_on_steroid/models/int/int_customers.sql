WITH cte_source AS (
    SELECT *
    FROM
        {{ ref('stg_customers') }}
)
,
cte_final AS (
    SELECT
        created_at,
        lower(trim(customer_id)) AS customer_id,
        lower(trim(customer_name)) AS customer_name,
        lower(trim(email)) AS email
    FROM cte_source
)

SELECT * FROM cte_final
