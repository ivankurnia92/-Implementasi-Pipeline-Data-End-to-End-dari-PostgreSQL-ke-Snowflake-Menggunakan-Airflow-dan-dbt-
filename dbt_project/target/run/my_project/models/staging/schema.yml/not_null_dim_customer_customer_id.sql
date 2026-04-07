select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_id
from DBT_DEV_DB.PUBLIC_2_STAGING.dim_customer
where customer_id is null



      
    ) dbt_internal_test