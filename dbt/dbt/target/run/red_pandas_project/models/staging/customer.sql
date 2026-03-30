
  create view "postgres"."public_staging"."customer__dbt_tmp"
    
    
  as (
    

with source as (
    select * from "eccom_db"."staging"."customer"
),

cleaned as (
    select
        id                              as customer_id,
        created_time::timestamp         as created_at,
        trim(full_name)                 as full_name,
        trim(salulation)                as salutation,
        trim(first_name)                as first_name,
        trim(last_name)                 as last_name,
        street_number::int              as street_number,
        trim(street_name)               as street_name,
        trim(city)                      as city,
        trim(postcode)                  as postcode,
        upper(trim(state))              as state,
        trim(full_address)              as full_address
    from source
    where id is not null
)

select * from cleaned
  );