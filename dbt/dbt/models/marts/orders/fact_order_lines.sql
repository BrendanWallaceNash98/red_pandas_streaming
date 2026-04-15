{{
    config(
        materialized='table'
    )
}}

select
    order_id,
    customer_id,
    product_id,
    product_name,
    unit_price,
    quantity,
    line_total,
    created_at
from {{ ref('stg_order_lines') }}
