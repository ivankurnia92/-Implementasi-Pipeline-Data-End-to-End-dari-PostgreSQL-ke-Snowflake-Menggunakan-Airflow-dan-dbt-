{{ config(materialized='table') }}

with raw_snapshot as (
    select * from {{ ref('employee_snapshot') }}
),

deduplicated as (
    select 
        *,
        -- Gunakan "ID" (sesuai nama kolom asli di source/snapshot)
        row_number() over (
            partition by ID 
            order by dbt_updated_at desc
        ) as row_num
    from raw_snapshot
),

final as (
    select
        -- Alias ID menjadi EMPLOYEE_ID untuk konsistensi di level Marts
        ID as EMPLOYEE_ID,
        
        FIRST_NAME,
        LAST_NAME,
        concat(FIRST_NAME, ' ', LAST_NAME) as FULL_NAME,
        
        COMPANY,
        EMAIL_ADDRESS,
        JOB_TITLE,
        MOBILE_PHONE,
        
        ADDRESS,
        CITY,
        STATE_PROVINCE,
        ZIP_POSTAL_CODE,
        COUNTRY_REGION,
        
        DBT_VALID_FROM as EFFECTIVE_START_DATE,
        DBT_VALID_TO as EFFECTIVE_END_DATE,
        
        case 
            when row_num = 1 and DBT_VALID_TO is null then 'CURRENT'
            else 'HISTORICAL'
        end as ROW_STATUS

    from deduplicated
)

select * from final