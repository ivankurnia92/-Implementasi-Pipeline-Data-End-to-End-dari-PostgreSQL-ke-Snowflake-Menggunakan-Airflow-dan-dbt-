
  
    

        create or replace transient table DBT_DEV_DB.PUBLIC_3_MART.product_stock_turnover
         as
        (

with fact_inventory as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.fact_inventory
),

dim_product as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product where ROW_STATUS = 'CURRENT'
),

inventory_metrics as (
    select
        PRODUCT_ID,
        -- 1. Menghitung Stok Keluar (Sold)
        sum(case when TRANSACTION_TYPE_NAME = 'Sold' then abs(QUANTITY) else 0 end) as TOTAL_UNITS_SOLD,
        
        -- 2. Menghitung Stok Masuk (Purchased)
        sum(case when TRANSACTION_TYPE_NAME = 'Purchased' then QUANTITY else 0 end) as TOTAL_UNITS_PURCHASED,
        
        -- 3. Estimasi Stok Rata-rata (Average Inventory)
        -- Dalam praktek nyata, ini biasanya (Stok Awal + Stok Akhir) / 2
        avg(QUANTITY) as AVG_STOCK_LEVEL,
        
        count(distinct TRANSACTION_DATE) as ACTIVE_DAYS

    from fact_inventory
    group by 1
)

select
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    m.TOTAL_UNITS_SOLD,
    m.TOTAL_UNITS_PURCHASED,
    
    -- 4. Perhitungan Turnover Ratio
    -- Rumus: Total Unit Terjual / Rata-rata Stok
    round(
        m.TOTAL_UNITS_SOLD / nullif(m.AVG_STOCK_LEVEL, 0), 
        2
    ) as STOCK_TURNOVER_RATIO,
    
    -- 5. Days Sales of Inventory (DSI)
    -- Berapa hari rata-rata barang mengendap di gudang sebelum laku
    round(
        nullif(m.AVG_STOCK_LEVEL, 0) / nullif(m.TOTAL_UNITS_SOLD / nullif(m.ACTIVE_DAYS, 0), 0), 
        1
    ) as AVG_DAYS_TO_SELL,

    case 
        when (m.TOTAL_UNITS_SOLD / nullif(m.AVG_STOCK_LEVEL, 0)) > 5 then 'High Turnover'
        when (m.TOTAL_UNITS_SOLD / nullif(m.AVG_STOCK_LEVEL, 0)) between 2 and 5 then 'Moderate'
        else 'Low Turnover / Slow Moving'
    end as TURNOVER_CATEGORY

from inventory_metrics m
left join dim_product p on m.product_id = p.product_id
where p.product_id is not null
        );
      
  