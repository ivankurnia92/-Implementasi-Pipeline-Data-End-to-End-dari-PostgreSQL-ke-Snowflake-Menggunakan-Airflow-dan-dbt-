{% snapshot employee_snapshot %}

{{
    config(
      target_database='DBT_DEV_DB',
      target_schema='SNAPSHOTS',
      unique_key='ID',
      strategy='check',
      check_cols=['ADDRESS']
    )
}}

-- Mengikuti pola yang berhasil: menulis kolom secara eksplisit (Caps)
SELECT 
    ID, 
    FIRST_NAME,
    LAST_NAME,
    COMPANY,
    EMAIL_ADDRESS,
    JOB_TITLE,
    BUSINESS_PHONE,
    MOBILE_PHONE,
    FAX_NUMBER,
    ADDRESS,
    CITY,
    STATE_PROVINCE,
    ZIP_POSTAL_CODE,
    COUNTRY_REGION,
FROM {{ source('snowflake_raw', 'EMPLOYEES') }}

{% endsnapshot %}