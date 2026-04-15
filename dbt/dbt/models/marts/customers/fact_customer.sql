{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customer') }}
),

order_summary as (
    select
        customer_id,
        count(distinct order_id)        as total_orders,
        sum(order_quantity::int)        as total_items_ordered,
        min(created_at)                 as first_order_at,
        max(created_at)                 as last_order_at
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.created_at,
    c.full_name,
    c.salutation,
    c.first_name,
    c.last_name,
    c.street_number,
    c.street_name,
    c.city,
    c.postcode,
    c.state,
    c.full_address,

    -- Order metrics enriched onto the customer
    coalesce(o.total_orders, 0)         as total_orders,
    coalesce(o.total_items_ordered, 0)  as total_items_ordered,
    o.first_order_at,
    o.last_order_at,

    -- Derived
    case
        when o.total_orders is null then 'no_orders'
        when o.total_orders = 1 then 'new'
        when o.total_orders between 2 and 5 then 'returning'
        else 'loyal'
    end as customer_segment

from customers c
left join order_summary o using (customer_id)
