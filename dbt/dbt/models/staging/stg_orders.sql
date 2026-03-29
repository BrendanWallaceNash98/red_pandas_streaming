{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        id                                  as order_id,
        created_date::timestamp             as created_at,
        customer_id,
        order_products::jsonb               as order_products,
        order_quantity::jsonb               as order_quantity
    from source
    where id is not null
)

select * from cleaned
