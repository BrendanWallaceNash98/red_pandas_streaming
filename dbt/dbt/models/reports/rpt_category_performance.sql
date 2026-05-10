{{
    config(
        materialized='view',
        tags=['reporting', 'products', 'categories']
    )
}}

with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

product_category as (
    select * from {{ ref('stg_products') }}
),

category_metrics as (
    select
        pc.category,
        sum(ol.line_total)                as total_revenue,
        sum(ol.quantity)                  as total_quantity_sold,
        count(distinct ol.order_id)       as total_orders,
        count(distinct ol.product_id)     as unique_products_sold,
        round(avg(ol.unit_price)::numeric, 2) as avg_unit_price,
        round(avg(ol.line_total)::numeric, 2) as avg_line_value
    from order_lines ol
    join product_category pc on ol.product_id = pc.product_id
    group by pc.category
)

select
    cm.*,
    round(
        (cm.total_revenue * 100.0) /
        (select sum(total_revenue) from category_metrics)::numeric,
        2
    ) as revenue_share_pct,
    round(
        (cm.total_quantity_sold * 100.0) /
        (select sum(total_quantity_sold) from category_metrics)::numeric,
        2
    ) as quantity_share_pct,
    rank() over (order by total_revenue desc) as revenue_rank

from category_metrics cm
order by total_revenue desc
