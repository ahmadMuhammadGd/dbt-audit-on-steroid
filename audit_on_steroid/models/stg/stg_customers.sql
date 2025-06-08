SELECT
    customer_id::TEXT,
    customer_name::TEXT,
    email::TEXT,
    NOW() AS created_at
FROM {{ ref('raw_customers') }}
