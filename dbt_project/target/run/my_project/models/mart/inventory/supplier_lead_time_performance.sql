
  
    

        create or replace transient table DBT_DEV_DB.PUBLIC_3_MART.supplier_lead_time_performance
         as
        (

with fact_purchase_order as (
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.fact_purchase_order
),

supplier_metrics as (
    select
        SUPPLIER_ID,
        -- Menggunakan TRY_TO_DATE untuk menghindari error jika format data aneh
        -- dan memastikan kita bekerja dengan objek DATE yang valid
        try_to_date(to_varchar(CREATION_DATE)) as PO_DATE,
        try_to_date(to_varchar(EXPECTED_DATE)) as DUE_DATE,
        try_to_date(to_varchar(DATE_RECEIVED)) as RECV_DATE,
        
        -- 1. Menghitung Selisih Hari (Lead Time)
        datediff('day', 
            try_to_date(to_varchar(CREATION_DATE)), 
            try_to_date(to_varchar(DATE_RECEIVED))
        ) as ACTUAL_LEAD_TIME,
        
        -- 2. Menghitung Deviasi (Delay)
        datediff('day', 
            try_to_date(to_varchar(EXPECTED_DATE)), 
            try_to_date(to_varchar(DATE_RECEIVED))
        ) as DAYS_DIFF_FROM_EXPECTED,
        
        QUANTITY,
        UNIT_COST

    from fact_purchase_order
    -- Filter baris yang memiliki komponen tanggal lengkap
    where DATE_RECEIVED is not null 
      and CREATION_DATE is not null
      and EXPECTED_DATE is not null
),

final_agg as (
    select
        SUPPLIER_ID,
        round(avg(ACTUAL_LEAD_TIME), 1) as AVG_LEAD_TIME_DAYS,
        round(avg(DAYS_DIFF_FROM_EXPECTED), 1) as AVG_DELAY_DAYS,
        count(*) as TOTAL_SHIPMENTS_RECEIVED,
        
        -- Menghitung On-Time Delivery Rate
        round(
            count(case when DAYS_DIFF_FROM_EXPECTED <= 0 then 1 end) * 100.0 / nullif(count(*), 0), 
            2
        ) as ON_TIME_DELIVERY_PCT

    from supplier_metrics
    -- Menghilangkan baris yang gagal di-convert tanggalnya (null dari try_to_date)
    where PO_DATE is not null 
      and DUE_DATE is not null 
      and RECV_DATE is not null
    group by 1
)

select
    s.*,
    -- 3. Segmentasi Supplier Berdasarkan Performa
    case 
        when s.ON_TIME_DELIVERY_PCT >= 90 then 'Class A (Reliable)'
        when s.ON_TIME_DELIVERY_PCT >= 75 then 'Class B (Average)'
        else 'Class C (Risky - Frequent Delays)'
    end as SUPPLIER_RATING

from final_agg s
order by s.ON_TIME_DELIVERY_PCT desc
        );
      
  