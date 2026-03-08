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
        id as order_id,
        case 
            when created_time is not null 
            then created_time::timestamp
            else null
        end as created_at,
        customer_id,
        order_products,
        order_quantity::INT
    from source
    where id is not null
)

select * from cleaned
