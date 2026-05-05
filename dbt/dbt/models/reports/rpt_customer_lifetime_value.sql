{{
    config(
        materialized='view',
        tags=['reporting', 'customers', 'ltv']
    )
}}

with order_metrics as (
    select
        customer_id,
        count(distinct order_id)          as total_orders,
        sum(line_total)                   as total_spent,
        avg(line_total / nullif(quantity, 0)) as avg_line_value,
        min(created_at)                   as first_order_at,
        max(created_at)                   as last_order_at
    from {{ ref('stg_order_lines') }}
    group by customer_id
),

customer_info as (
    select * from {{ ref('stg_customer') }}
),

ltv as (
    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.city,
        c.state,

        o.total_orders,
        round(o.total_spent::numeric, 2) as total_spent,
        round(o.total_spent / o.total_orders::numeric, 2) as avg_order_value,
        o.first_order_at,
        o.last_order_at,

        -- Customer tenure in days
        (o.last_order_at - o.first_order_at)::int as customer_tenure_days,

        -- Average daily spend (if tenure > 0)
        case
            when (o.last_order_at - o.first_order_at)::int > 0
            then round(o.total_spent / ((o.last_order_at - o.first_order_at)::numeric), 2)
            else 0
        end as avg_daily_spend

    from customer_info c
    join order_metrics o using (customer_id)
)

select
    *,

    -- Simple LTV tier
    case
        when total_spent >= 2000 then 'platinum'
        when total_spent >= 1000 then 'gold'
        when total_spent >= 500  then 'silver'
        else 'bronze'
    end as ltv_tier

from ltv
order by total_spent desc
