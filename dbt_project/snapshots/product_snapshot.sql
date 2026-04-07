{% snapshot product_snapshot %}

{{
    config(
      target_database='DBT_DEV_DB',
      target_schema='SNAPSHOTS',
      unique_key='ID',
      strategy='check',
      check_cols=['LIST_PRICE', 'STANDARD_COST']
    )
}}

-- Langsung ambil dari source snowflake_raw (pastikan di sources.yml sudah ada 'products')
SELECT * FROM {{ source('snowflake_raw', 'PRODUCTS') }}

{% endsnapshot %}