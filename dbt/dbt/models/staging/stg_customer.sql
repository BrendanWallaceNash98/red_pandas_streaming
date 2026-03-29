{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('raw', 'customer') }}
),

cleaned as (
    select
        id as customer_id,
        
        -- Parse created_time to timestamp
        -- Adjust the format based on your actual data format
        case 
            when created_time is not null 
            then created_time::timestamp
            else null
        end as created_at,
        
        -- Name fields
        trim(full_name) as full_name,
        trim(salulation) as salutation, 
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        
        -- Address fields
        street_number,
        trim(street_name) as street_name,
        trim(city) as city,
        trim(postcode) as postcode,
        upper(trim(state)) as state,
        trim(full_address) as full_address
        
    from source
    
    -- Data quality filters
    where id is not null
        -- Add state validation if needed
        and (state is null or length(state) <= 2)
)

select * from cleaned
