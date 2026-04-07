{{ config(
    materialized='table',
    schema='PUBLIC_3_MART'
) }}

with monthly_sales as (
    -- Mengambil total penjualan per bulan
    select
        date_trunc('month', ORDER_DATE) as BUSINESS_MONTH,
        sum(NET_REVENUE) as TOTAL_SALES_VALUE,
        sum(QUANTITY) as TOTAL_UNITS_SOLD
    from {{ ref('fact_sales') }}
    group by 1
),

monthly_purchases as (
    -- Mengambil total pembelian ke supplier per bulan
    select
        date_trunc('month', try_to_date(to_varchar(DATE_RECEIVED))) as BUSINESS_MONTH,
        sum(QUANTITY * UNIT_COST) as TOTAL_PURCHASE_VALUE,
        sum(QUANTITY) as TOTAL_UNITS_PURCHASED
    from {{ ref('fact_purchase_order') }}
    where DATE_RECEIVED is not null
    group by 1
)

select
    coalesce(s.BUSINESS_MONTH, p.BUSINESS_MONTH) as REPORT_MONTH,
    
    -- Metrik Penjualan
    coalesce(s.TOTAL_SALES_VALUE, 0) as SALES_VALUE,
    coalesce(s.TOTAL_UNITS_SOLD, 0) as UNITS_SOLD,
    
    -- Metrik Pembelian
    coalesce(p.TOTAL_PURCHASE_VALUE, 0) as PURCHASE_VALUE,
    coalesce(p.TOTAL_UNITS_PURCHASED, 0) as UNITS_PURCHASED,
    
    -- 1. Sales to Purchase Value Ratio
    -- Rumus: Revenue / Pengeluaran Stok
    round(
        coalesce(s.TOTAL_SALES_VALUE, 0) / nullif(p.TOTAL_PURCHASE_VALUE, 0), 
        2
    ) as VALUE_RATIO,
    
    -- 2. Inventory Absorption Rate
    -- Seberapa banyak unit yang terjual dibandingkan yang masuk
    round(
        (coalesce(s.TOTAL_UNITS_SOLD, 0) / nullif(p.TOTAL_UNITS_PURCHASED, 0)) * 100, 
        2
    ) as ABSORPTION_PCT,

    -- 3. Status Cash Flow Barang
    case 
        when (coalesce(s.TOTAL_SALES_VALUE, 0) / nullif(p.TOTAL_PURCHASE_VALUE, 0)) > 1.2 then 'Positive Flow (Profitable)'
        when (coalesce(s.TOTAL_SALES_VALUE, 0) / nullif(p.TOTAL_PURCHASE_VALUE, 0)) between 0.9 and 1.2 then 'Balanced'
        else 'Heavy Purchasing (Stockpiling)'
    end as CASH_FLOW_STATUS

from monthly_sales s
full outer join monthly_purchases p on s.BUSINESS_MONTH = p.BUSINESS_MONTH
order by REPORT_MONTH desc