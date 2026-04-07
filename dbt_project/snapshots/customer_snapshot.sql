{% snapshot customer_snapshot %}

{{
    config(
      target_database='DBT_DEV_DB',
      target_schema='SNAPSHOTS',
      unique_key='ID',
      strategy='check',
      check_cols=['COMPANY','FIRST_NAME','ADDRESS']
    )
}}


SELECT 
    ID, 
    COMPANY, 
    LAST_NAME,  
    FIRST_NAME,
    JOB_TITLE,
    BUSINESS_PHONE,
    ADDRESS,
    CITY,
    STATE_PROVINCE
FROM {{ source('snowflake_raw', 'CUSTOMER') }}

{% endsnapshot %}