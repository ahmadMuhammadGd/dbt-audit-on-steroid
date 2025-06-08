WITH cte_source AS (
    SELECT
        product_id,
        product_name,
        price,
        created_at
    FROM {{ ref('stg_products') }}

),

cte_final AS (
    SELECT
        LOWER(TRIM(product_id)) AS product_id,
        LOWER(TRIM(product_name)) AS product_name,
        {{ cast__str_to_numeric('price') }} AS price,
        created_at
    FROM cte_source
)

SELECT * FROM cte_final
