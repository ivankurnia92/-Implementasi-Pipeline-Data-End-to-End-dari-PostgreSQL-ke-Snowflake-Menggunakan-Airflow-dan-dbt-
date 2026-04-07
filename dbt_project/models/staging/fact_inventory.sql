{{ config(
    materialized='table'
) }}

with source_data as (
    -- Deduplikasi di level source untuk menghilangkan 102 error duplikat
    select distinct
        ID as INVENTORY_TRANSACTION_ID,
        PRODUCT_ID,
        PURCHASE_ORDER_ID,
        CUSTOMER_ORDER_ID,
        TRANSACTION_TYPE,
        QUANTITY,
        COMMENTS,
        CAST(TRANSACTION_CREATED_DATE AS DATE) as TRANSACTION_DATE,
        TRANSACTION_CREATED_DATE as TRANSACTION_CREATED_AT,
        TRANSACTION_MODIFIED_DATE as TRANSACTION_MODIFIED_AT
    from {{ source('snowflake_raw','INVENTORY_TRANSACTIONS') }}
),

dim_product as (
    -- Mengambil referensi produk yang aktif saja
    select * from {{ ref('dim_product') }} where ROW_STATUS = 'CURRENT'
)

select 
    s.*,
    -- 1. Menambahkan kategori transaksi (Business Logic)
    case 
        when s.TRANSACTION_TYPE = 1 then 'Purchased'
        when s.TRANSACTION_TYPE = 2 then 'Sold'
        when s.TRANSACTION_TYPE = 3 then 'On Hold'
        when s.TRANSACTION_TYPE = 4 then 'Waste'
        else 'Unknown'
    end as TRANSACTION_TYPE_NAME,

    -- 2. Metadata Audit
    CURRENT_TIMESTAMP() as INSERTION_TIMESTAMP

from source_data s
-- Melakukan join ke dimensi untuk memastikan integritas data
left join dim_product p 
    on s.product_id = p.product_id

-- Opsional: filter jika Anda ingin membuang transaksi yang produknya tidak terdaftar
-- where p.product_id is not null