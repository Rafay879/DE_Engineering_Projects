WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

translation AS (
    SELECT * FROM {{ source('silver', 'product_category_name_translation') }}
),

order_items AS (
    SELECT
        product_id,
        COUNT(*)              AS times_ordered,
        SUM(price)            AS total_revenue,
        AVG(price)            AS avg_price
    FROM {{ ref('stg_order_items') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_category_name,
    COALESCE(t.product_category_name_english,
             p.product_category_name)       AS product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    COALESCE(oi.times_ordered, 0)           AS times_ordered,
    COALESCE(oi.total_revenue, 0)           AS total_revenue,
    COALESCE(oi.avg_price, 0)              AS avg_price,
    CURRENT_TIMESTAMP                       AS dbt_updated_at
FROM products p
LEFT JOIN translation t
    ON p.product_category_name = t.product_category_name
LEFT JOIN order_items oi
    ON p.product_id = oi.product_id