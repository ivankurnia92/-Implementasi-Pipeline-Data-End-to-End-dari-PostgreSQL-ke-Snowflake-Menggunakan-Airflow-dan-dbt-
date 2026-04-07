{{ config(
    materialized='table'
) }}

with fact_sales as (
    select * from {{ ref('fact_sales') }}
),

monthly_aggregation as (
    select
        -- 1. Dimensi Waktu (Agregasi per Bulan)
        date_trunc('month', ORDER_DATE) as SALES_MONTH,
        
        -- 2. Metrik Volume
        count(distinct ORDER_ID) as TOTAL_ORDERS,
        count(distinct CUSTOMER_ID) as TOTAL_CUSTOMERS,
        sum(QUANTITY) as TOTAL_UNITS_SOLD,
        
        -- 3. Metrik Finansial
        round(sum(NET_REVENUE), 2) as MONTHLY_REVENUE,
        
        -- 4. Rata-rata Nilai Transaksi (AOV)
        round(sum(NET_REVENUE) / nullif(count(distinct ORDER_ID), 0), 2) as AVG_ORDER_VALUE

    from fact_sales
    group by 1
),

final_with_growth as (
    select
        *,
        -- 5. Menghitung Revenue Bulan Sebelumnya (untuk Growth Analysis)
        lag(MONTHLY_REVENUE) over (order by SALES_MONTH) as PREVIOUS_MONTH_REVENUE,
        
        -- 6. Menghitung Persentase Pertumbuhan (MoM Growth %)
        round(
            (MONTHLY_REVENUE - lag(MONTHLY_REVENUE) over (order by SALES_MONTH)) 
            / nullif(lag(MONTHLY_REVENUE) over (order by SALES_MONTH), 0) * 100, 
            2
        ) as MOM_REVENUE_GROWTH_PCT

    from monthly_aggregation
)

select * from final_with_growth
order by SALES_MONTH desc