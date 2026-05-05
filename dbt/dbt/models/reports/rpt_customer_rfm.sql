{{
    config(
        materialized='view',
        tags=['reporting', 'customers', 'rfm']
    )
}}

with order_metrics as (
    select
        customer_id,
        count(distinct order_id)                          as order_frequency,
        max(created_at)                                   as last_order_at,
        min(created_at)                                   as first_order_at,
        sum(line_total)                                   as total_spent,
        avg(line_total / nullif(quantity, 0))             as avg_line_value
    from {{ ref('stg_order_lines') }}
    group by customer_id
),

customer_info as (
    select * from {{ ref('stg_customer') }}
),

rfm as (
    select
        c.customer_id,
        c.full_name,
        c.first_name,
        c.last_name,
        c.city,
        c.state,
        c.first_name || ' ' || c.last_name as display_name,

        -- Recency: days since last order
        (current_date - o.last_order_at)::int as recency_days,

        -- Frequency: total number of orders
        o.order_frequency,

        -- Monetary: total amount spent
        round(o.total_spent::numeric, 2) as total_spent,

        -- Average order value
        round(o.total_spent / o.order_frequency::numeric, 2) as avg_order_value,

        -- Customer tenure in days
        (o.last_order_at - o.first_order_at)::int as customer_tenure_days

    from customer_info c
    join order_metrics o using (customer_id)
)

select
    *,

    -- Simple RFM segment assignment
    case
        when recency_days <= 30 and order_frequency >= 3 then 'champions'
        when recency_days <= 60 and order_frequency >= 2 then 'loyal'
        when recency_days <= 90 then 'at_risk'
        when recency_days <= 180 then 'need_attention'
        else 'hibernating'
    end as rfm_segment

from rfm
order by total_spent desc
