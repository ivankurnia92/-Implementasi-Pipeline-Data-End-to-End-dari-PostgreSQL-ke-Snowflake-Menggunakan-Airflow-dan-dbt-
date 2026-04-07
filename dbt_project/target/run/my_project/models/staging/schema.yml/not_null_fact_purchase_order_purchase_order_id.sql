select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select purchase_order_id
from DBT_DEV_DB.PUBLIC_2_STAGING.fact_purchase_order
where purchase_order_id is null



      
    ) dbt_internal_test