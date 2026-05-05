{{
    config(
        materialized='view',
        tags=['reporting', 'products']
    )
}}

with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

product_metrics as (
    select
        product_id,
        product_name,
        sum(quantity)                       as total_quantity_sold,
        sum(line_total)                     as total_revenue,
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as unique_customers,
        round(avg(unit_price)::numeric, 2)  as avg_unit_price,
        round(min(unit_price)::numeric, 2)  as min_unit_price,
        round(max(unit_price)::numeric, 2)  as max_unit_price
    from order_lines
    group by product_id, product_name
)

select
    pm.*,

    -- Revenue share
    round(
        (pm.total_revenue * 100.0) /
        (select sum(total_revenue) from product_metrics)::numeric,
        2
    ) as revenue_share_pct,

    -- Rank by revenue
    rank() over (order by total_revenue desc) as revenue_rank,

    -- Rank by quantity
    rank() over (order by total_quantity_sold desc) as quantity_rank

from product_metrics pm
order by total_revenue desc
