WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT
        customer_id,
        COUNT(*)                           AS total_orders,
        MIN(order_purchase_timestamp)      AS first_order_date,
        MAX(order_purchase_timestamp)      AS last_order_date
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    COALESCE(o.total_orders, 0)            AS total_orders,
    o.first_order_date,
    o.last_order_date,
    CURRENT_TIMESTAMP                      AS dbt_updated_at
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id