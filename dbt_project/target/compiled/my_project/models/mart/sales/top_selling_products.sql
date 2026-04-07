

with fact_sales as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.fact_sales
),

dim_product as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product where ROW_STATUS = 'CURRENT'
),

product_metrics as (
    select
        PRODUCT_ID,
        -- 1. Metrik Penjualan
        count(distinct ORDER_ID) as TOTAL_ORDERS_COUNT,
        sum(QUANTITY) as TOTAL_QUANTITY_SOLD,
        round(sum(NET_REVENUE), 2) as TOTAL_REVENUE,
        
        -- 2. Harga Rata-rata & Diskon
        round(avg(UNIT_PRICE), 2) as AVG_UNIT_PRICE,
        round(avg(DISCOUNT) * 100, 2) as AVG_DISCOUNT_PERCENT,
        
        -- 3. Waktu Penjualan
        min(ORDER_DATE) as FIRST_SOLD_DATE,
        max(ORDER_DATE) as LAST_SOLD_DATE

    from fact_sales
    group by 1
)

select
    -- Join dengan Dimensi untuk Informasi Produk
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.PRODUCT_CODE,
    p.DISCONTINUED_STATUS,
    
    -- Metrik dari CTE
    m.TOTAL_ORDERS_COUNT,
    m.TOTAL_QUANTITY_SOLD,
    m.TOTAL_REVENUE,
    m.AVG_UNIT_PRICE,
    m.AVG_DISCOUNT_PERCENT,
    m.FIRST_SOLD_DATE,
    m.LAST_SOLD_DATE,
    
    -- 4. Peringkat (Ranking) berdasarkan Revenue
    dense_rank() over (order by m.TOTAL_REVENUE desc) as REVENUE_RANK

from product_metrics m
left join dim_product p on m.product_id = p.product_id
where p.product_id is not null