{{ config(
    materialized='table'
) }}

with orders as (
    select * from {{ source('snowflake_raw', 'ORDERS') }}
),

order_details as (
    select * from {{ source('snowflake_raw', 'ORDER_DETAILS') }}
),

-- Referensi Dimensi yang Aktif
dim_product as (
    select * from {{ ref('dim_product') }} where ROW_STATUS = 'CURRENT'
),

dim_customer as (
    select * from {{ ref('dim_customer') }} where ROW_STATUS = 'CURRENT'
),

dim_employee as (
    select * from {{ ref('dim_employee') }} where ROW_STATUS = 'CURRENT'
)

select
    -- 1. Keys (Primary & Foreign Keys)
    -- Menggunakan huruf besar (Uppercase) agar aman di Snowflake
    od.ORDER_ID,
    od.PRODUCT_ID,
    o.CUSTOMER_ID,
    o.EMPLOYEE_ID,
    o.SHIPPER_ID,
    od.PURCHASE_ORDER_ID,
    od.INVENTORY_ID,

    -- 2. Sales Metrics
    od.QUANTITY,
    od.UNIT_PRICE,
    od.DISCOUNT,
    -- Kalkulasi Revenue dengan pembulatan 2 desimal
    round((od.QUANTITY * od.UNIT_PRICE) * (1 - coalesce(od.DISCOUNT, 0)), 2) as NET_REVENUE,
    
    -- 3. Status & Dates
    od.STATUS_ID,
    CAST(o.ORDER_DATE AS DATE) as ORDER_DATE,
    o.SHIPPED_DATE,
    o.PAID_DATE,
    od.DATE_ALLOCATED,
    
    -- 4. Audit Metadata
    CURRENT_TIMESTAMP() as INSERTION_TIMESTAMP

from orders o
-- Menggunakan INNER JOIN agar hanya order yang punya detail yang masuk
inner join order_details od
    on o.ID = od.ORDER_ID

-- Join ke Dimensi (Opsional untuk pengecekan integritas di level query)
left join dim_product p on od.PRODUCT_ID = p.PRODUCT_ID
left join dim_customer c on o.CUSTOMER_ID = c.CUSTOMER_ID
left join dim_employee e on o.EMPLOYEE_ID = e.EMPLOYEE_ID