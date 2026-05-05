{{
    config(
        materialized='view',
        tags=['reporting', 'sales', 'daily']
    )
}}

with order_lines as (
    select * from {{ ref('stg_order_lines') }}
)

select
    date(created_at)                          as report_date,
    count(distinct order_id)                  as total_orders,
    count(*)                                  as total_items_sold,
    sum(line_total)                           as total_revenue,
    avg(line_total / nullif(quantity, 0))     as avg_line_value,
    sum(line_total) / count(distinct order_id) as avg_order_value
from order_lines
group by date(created_at)
order by report_date desc
