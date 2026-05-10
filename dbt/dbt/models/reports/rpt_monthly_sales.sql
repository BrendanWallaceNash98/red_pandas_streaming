{{
    config(
        materialized='view',
        tags=['reporting', 'sales', 'monthly']
    )
}}

with daily as (
    select * from {{ ref('rpt_daily_sales') }}
),

monthly as (
    select
        date_trunc('month', report_date)::date    as month,
        sum(total_orders)                         as total_orders,
        sum(total_revenue)                        as total_revenue,
        sum(total_items_sold)                     as total_items_sold,
        sum(total_revenue) / sum(total_orders)    as avg_order_value
    from daily
    group by date_trunc('month', report_date)
)

select
    month,
    total_orders,
    total_revenue,
    total_items_sold,
    avg_order_value,
    sum(total_revenue) over (order by month)        as cumulative_revenue,
    sum(total_orders) over (order by month)         as cumulative_orders
from monthly
order by month
