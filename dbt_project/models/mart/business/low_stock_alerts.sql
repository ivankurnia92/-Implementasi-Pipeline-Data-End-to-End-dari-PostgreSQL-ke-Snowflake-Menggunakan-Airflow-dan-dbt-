{{ config(
    materialized='table',
    schema='PUBLIC_3_MART'
) }}

with daily_sales_velocity as (
    -- 1. Menghitung rata-rata penjualan harian dalam 30 hari terakhir
    select
        PRODUCT_ID,
        sum(QUANTITY) / 30 as AVG_DAILY_SALES_VELOCITY
    from {{ ref('fact_sales') }}
    where ORDER_DATE >= dateadd('day', -30, current_date())
    group by 1
),

current_inventory as (
    -- 2. Mengambil saldo stok saat ini
    select
        PRODUCT_ID,
        sum(QUANTITY) as CURRENT_STOCK_ON_HAND
    from {{ ref('fact_inventory') }}
    group by 1
),

dim_product as (
    select * from {{ ref('dim_product') }} where ROW_STATUS = 'CURRENT'
)

select
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    i.CURRENT_STOCK_ON_HAND,
    round(v.AVG_DAILY_SALES_VELOCITY, 2) as DAILY_VELOCITY,
    
    -- 3. Menghitung Days of Inventory (DOI)
    -- Stok Sekarang / Rata-rata Penjualan Harian
    case 
        when coalesce(v.AVG_DAILY_SALES_VELOCITY, 0) = 0 then 999 -- Stok aman karena tidak ada penjualan
        else round(i.CURRENT_STOCK_ON_HAND / v.AVG_DAILY_SALES_VELOCITY, 1)
    end as DAYS_UNTIL_OUT_OF_STOCK,

    -- 4. Logika Alert
    case 
        when i.CURRENT_STOCK_ON_HAND <= 0 then 'CRITICAL: OUT OF STOCK'
        when (i.CURRENT_STOCK_ON_HAND / nullif(v.AVG_DAILY_SALES_VELOCITY, 0)) <= 7 then 'URGENT: REORDER NOW ( < 7 Days)'
        when (i.CURRENT_STOCK_ON_HAND / nullif(v.AVG_DAILY_SALES_VELOCITY, 0)) <= 14 then 'WARNING: LOW STOCK ( < 14 Days)'
        else 'HEALTHY'
    end as ALERT_STATUS

from current_inventory i
left join daily_sales_velocity v on i.product_id = v.product_id
left join dim_product p on i.product_id = p.product_id
-- Kita hanya tampilkan yang berstatus peringatan
where ALERT_STATUS != 'HEALTHY'
  and p.PRODUCT_ID is not null
order by DAYS_UNTIL_OUT_OF_STOCK asc