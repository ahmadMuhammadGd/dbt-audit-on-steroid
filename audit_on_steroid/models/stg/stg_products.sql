SELECT
    product_id::TEXT,
    product_name::TEXT,
    price::TEXT,
    NOW() AS created_at
FROM {{ ref('raw_products') }}
