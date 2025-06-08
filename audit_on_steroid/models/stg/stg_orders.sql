SELECT
    order_id::TEXT,
    customer_id::TEXT,
    order_date::TEXT,
    amount::TEXT,
    NOW() AS created_at
FROM {{ ref('raw_orders') }}
