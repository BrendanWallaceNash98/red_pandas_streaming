{{
    config(
        materialized='view'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

exploded as (
    select
        o.order_id,
        o.created_at,
        o.customer_id,
        p.key                               as product_name,
        p.value::numeric(10,2)              as unit_price,
        q.value::int                        as quantity,
        (p.value::numeric(10,2) * q.value::int) as line_total
    from orders o
    cross join lateral jsonb_each(o.order_products) as p(key, value)
    cross join lateral jsonb_each(o.order_quantity) as q(key, value)
    where p.key = q.key
),

final as (
    select
        e.order_id,
        e.created_at,
        e.customer_id,
        pr.product_id,
        e.product_name,
        e.unit_price,
        e.quantity,
        e.line_total
    from exploded e
    left join {{ ref('stg_products') }} pr
        on e.product_name = pr.product_name
)

select * from final
