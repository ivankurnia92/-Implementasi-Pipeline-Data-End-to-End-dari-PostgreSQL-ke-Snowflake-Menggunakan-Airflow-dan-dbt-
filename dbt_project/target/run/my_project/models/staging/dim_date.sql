
  
    

        create or replace transient table DBT_DEV_DB.PUBLIC_2_STAGING.dim_date
         as
        (

WITH CTE_MY_DATE AS (
    -- Menghasilkan data per jam mulai dari 2017-01-01
    SELECT DATEADD(HOUR, SEQ4(), '2017-01-01 00:00:00') AS MY_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 20000))
)

SELECT
    MY_DATE AS datetime_key,           -- Primary Key unik per jam
    TO_DATE(MY_DATE) AS full_date,     -- Tanggal saja
    TO_TIME(MY_DATE) AS time_of_day,   -- Jam saja
    YEAR(MY_DATE) AS year_num,
    MONTH(MY_DATE) AS month_num,
    MONTHNAME(MY_DATE) AS month_name,
    DAY(MY_DATE) AS day_of_month,
    DAYOFWEEK(MY_DATE) AS day_of_week,
    WEEKOFYEAR(MY_DATE) AS week_of_year,
    DAYOFYEAR(MY_DATE) AS day_of_year,
    HOUR(MY_DATE) AS hour_num,
    -- Tambahan berguna untuk filter laporan
    CASE WHEN DAYOFWEEK(MY_DATE) IN (6, 0) THEN TRUE ELSE FALSE END AS is_weekend
FROM CTE_MY_DATE
        );
      
  