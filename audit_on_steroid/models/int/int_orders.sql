WITH cte_source AS (
    SELECT *
    FROM
        {{ ref('stg_orders') }}
),

cte_final AS (
    SELECT
        TRIM(LOWER(order_id)) AS order_id,
        TRIM(LOWER(customer_id)) AS customer_id,
        CASE
            WHEN IS_DATE(order_date) THEN order_date::DATE
        END AS order_date,
        {{ cast__str_to_numeric('amount') }} AS amount,
        created_at
    FROM cte_source
)

SELECT * FROM cte_final
