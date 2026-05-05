{{
    config(
        materialized='view',
        tags=['reporting', 'geography']
    )
}}

with order_lines as (
    select * from {{ ref('stg_order_lines') }}
),

customer_info as (
    select
        customer_id,
        city,
        state
    from {{ ref('stg_customer') }}
),

geo as (
    select
        c.state,
        c.city,
        count(distinct ol.order_id)     as total_orders,
        sum(ol.line_total)              as total_revenue,
        sum(ol.quantity)                as total_items_sold,
        count(distinct ol.customer_id)  as unique_customers,
        round(avg(ol.line_total / nullif(ol.quantity, 0))::numeric, 2) as avg_line_value
    from order_lines ol
    join customer_info c using (customer_id)
    group by c.state, c.city
)

select
    g.*,

    -- Revenue share within state
    round(
        (g.total_revenue * 100.0) /
        (select sum(total_revenue) from geo)::numeric,
        2
    ) as revenue_share_pct,

    -- Rank by revenue
    rank() over (partition by g.state order by g.total_revenue desc) as city_revenue_rank,
    rank() over (order by g.total_revenue desc) as overall_revenue_rank

from geo g
order by total_revenue desc
