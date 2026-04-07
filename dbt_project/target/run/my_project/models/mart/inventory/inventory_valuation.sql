
  
    

        create or replace transient table DBT_DEV_DB.PUBLIC_3_MART.inventory_valuation
         as
        (

with fact_inventory as (
    -- Mengambil mutasi stok terakhir
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.fact_inventory
),

dim_product as (
    -- Mengambil data produk aktif dan harganya
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product where ROW_STATUS = 'CURRENT'
),

current_stock as (
    -- Menghitung saldo stok terakhir per produk
    select
        PRODUCT_ID,
        sum(QUANTITY) as TOTAL_ON_HAND,
        max(TRANSACTION_DATE) as LAST_MOVEMENT_DATE
    from fact_inventory
    group by 1
)

select
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.STANDARD_COST,
    s.TOTAL_ON_HAND,
    
    -- 1. Kalkulasi Nilai Inventaris (Stock x Cost)
    round(s.TOTAL_ON_HAND * p.STANDARD_COST, 2) as INVENTORY_VALUE,
    
    -- 2. Menentukan Status Stok
    case 
        when s.TOTAL_ON_HAND <= 0 then 'Out of Stock'
        when s.TOTAL_ON_HAND < 10 then 'Low Stock (Restock Soon)'
        else 'Healthy'
    end as STOCK_LEVEL_STATUS,
    
    s.LAST_MOVEMENT_DATE,
    current_timestamp() as VALUATION_TIMESTAMP

from current_stock s
left join dim_product p on s.product_id = p.product_id
where p.product_id is not null
  -- Kita hanya tampilkan produk yang masih memiliki nilai atau stok
  and (s.TOTAL_ON_HAND != 0 or p.STANDARD_COST > 0)
        );
      
  