{{ config(materialized='table') }}

with raw_snapshot as (
    select * from {{ ref('customer_snapshot') }}
),

deduplicated as (
    select 
        *,
        -- Gunakan kolom ID asli dari snapshot untuk partisi
        row_number() over (
            partition by ID 
            order by dbt_updated_at desc
        ) as row_num
    from raw_snapshot
),

final as (
    select
        ID as CUSTOMER_ID,
        COMPANY,
        LAST_NAME,
        FIRST_NAME,
        JOB_TITLE,
        BUSINESS_PHONE,
        ADDRESS,
        CITY,
        STATE_PROVINCE,
        -- Menentukan status baris
        case 
            when row_num = 1 and dbt_valid_to is null then 'CURRENT' 
            else 'HISTORICAL' 
        end as ROW_STATUS,
        dbt_valid_from as EFFECTIVE_DATE,
        dbt_valid_to as EXPIRATION_DATE
    from deduplicated
)

select * from final