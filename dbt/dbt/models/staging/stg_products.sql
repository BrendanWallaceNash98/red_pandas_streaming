{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ ref('products') }}
),

cleaned as (
    select
        product_id::int             as product_id,
        trim(product_name)          as product_name,
        price::numeric(10,2)        as price,
        trim(category)              as category
    from source
)

select * from cleaned
