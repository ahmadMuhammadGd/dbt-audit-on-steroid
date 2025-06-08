WITH orders AS (
    SELECT *
    FROM {{ ref('int_orders') }}
),

customers AS (
    SELECT *
    FROM {{ ref('int_customers') }}
)

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    o.order_date,
    o.amount,
    o.created_at AS order_created_at_in_dwh,
    c.created_at AS customer_created_at_in_dwh,
    NOW() AS created_at
FROM orders AS o
LEFT JOIN customers AS c
    ON o.customer_id = c.customer_id
