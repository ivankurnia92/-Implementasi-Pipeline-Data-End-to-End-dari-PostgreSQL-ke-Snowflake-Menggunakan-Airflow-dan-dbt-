
  
    

        create or replace transient table DBT_DEV_DB.PUBLIC_3_MART.customer_lifetime_value
         as
        (

with fact_sales as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.fact_sales
),

dim_customer as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_customer where ROW_STATUS = 'CURRENT'
),

customer_metrics as (
    select
        CUSTOMER_ID,
        -- 1. Rekapitulasi Waktu
        min(ORDER_DATE) as FIRST_PURCHASE_DATE,
        max(ORDER_DATE) as LAST_PURCHASE_DATE,
        datediff('day', min(ORDER_DATE), max(ORDER_DATE)) as CUSTOMER_TENURE_DAYS,
        
        -- 2. Rekapitulasi Transaksi
        count(distinct ORDER_ID) as TOTAL_ORDERS,
        sum(QUANTITY) as TOTAL_ITEMS_PURCHASED,
        
        -- 3. Rekapitulasi Finansial (CLV)
        round(sum(NET_REVENUE), 2) as TOTAL_REVENUE_CLV,
        round(avg(NET_REVENUE), 2) as AVG_ORDER_VALUE

    from fact_sales
    group by 1
)

select
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.COMPANY,
    c.CITY,
    m.FIRST_PURCHASE_DATE,
    m.LAST_PURCHASE_DATE,
    m.CUSTOMER_TENURE_DAYS,
    m.TOTAL_ORDERS,
    m.TOTAL_ITEMS_PURCHASED,
    m.TOTAL_REVENUE_CLV,
    m.AVG_ORDER_VALUE,
    
    -- 4. Segmentasi Sederhana
    case 
        when m.TOTAL_REVENUE_CLV > 5000 then 'VIP'
        when m.TOTAL_REVENUE_CLV > 2000 then 'Loyal'
        else 'Standard'
    end as CUSTOMER_SEGMENT

from customer_metrics m
left join dim_customer c on m.customer_id = c.customer_id
        );
      
  