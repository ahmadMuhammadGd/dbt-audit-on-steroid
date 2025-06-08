WITH customer_orders AS (
    SELECT
        customer_id,
        customer_name,
        email,
        order_id,
        order_date,
        amount
    FROM {{ ref('int_order_customer_joined') }}
),

agg AS (
    SELECT
        customer_id,
        customer_name,
        email,
        count(order_id) AS total_orders,
        sum(amount) AS total_spent,
        min(order_date) AS first_order,
        max(order_date) AS last_order,
        now() AS created_at
    FROM customer_orders
    GROUP BY customer_id, customer_name, email
)

SELECT * FROM agg
