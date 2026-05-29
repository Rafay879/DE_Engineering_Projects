{{
    config(
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='append_new_columns',
        tags=['daily', 'finance', 'critical'],
        post_hook="OPTIMIZE gold_db.fct_orders REWRITE DATA USING BIN_PACK"
    )
}}
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value)        AS total_payment_value,
        COUNT(*)                  AS payment_count,     --count all rows
        MAX(payment_type)         AS primary_payment_type
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

items AS (
    SELECT
        order_id,
        COUNT(*)                  AS item_count,
        SUM(price)                AS items_total,
        SUM(freight_value)        AS freight_total
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    COALESCE(i.item_count, 0)             AS item_count,
    COALESCE(i.items_total, 0)            AS items_total,
    COALESCE(i.freight_total, 0)          AS freight_total,
    COALESCE(p.total_payment_value, 0)    AS total_payment_value,
    COALESCE(p.payment_count, 0)          AS payment_count,
    COALESCE(p.primary_payment_type, 'unknown') AS primary_payment_type,

    DATE_DIFF(
        'day',
        o.order_purchase_timestamp,
        o.order_delivered_customer_date
    )                                     AS delivery_days,

    CASE
        WHEN o.order_delivered_customer_date
             <= o.order_estimated_delivery_date
        THEN true
        ELSE false
    END                                   AS delivered_on_time,

    o._run_date,
    o._batch_id

FROM orders o
LEFT JOIN payments p ON o.order_id = p.order_id
LEFT JOIN items    i ON o.order_id = i.order_id