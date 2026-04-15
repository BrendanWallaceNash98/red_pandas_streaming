{{
    config(
        materialized='table'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_lines as (
    select
        order_id,
        count(distinct product_id)  as distinct_products,
        sum(quantity)               as total_items,
        sum(line_total)             as order_total
    from {{ ref('stg_order_lines') }}
    group by order_id
)

select
    o.order_id,
    o.created_at,
    o.customer_id,
    ol.distinct_products,
    ol.total_items,
    ol.order_total

from orders o
left join order_lines ol using (order_id)
