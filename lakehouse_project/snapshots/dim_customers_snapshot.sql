-- snapshots/dim_customers_snapshot.sql
{% snapshot dim_customers_snapshot %}

{{
    config(
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_city',
            'customer_state',
            'customer_zip_code_prefix'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ ref('stg_customers') }}

{% endsnapshot %}