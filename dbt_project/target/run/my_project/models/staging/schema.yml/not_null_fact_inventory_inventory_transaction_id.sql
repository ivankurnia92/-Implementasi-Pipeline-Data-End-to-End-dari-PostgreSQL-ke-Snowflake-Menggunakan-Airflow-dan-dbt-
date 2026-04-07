select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select inventory_transaction_id
from DBT_DEV_DB.PUBLIC_2_STAGING.fact_inventory
where inventory_transaction_id is null



      
    ) dbt_internal_test