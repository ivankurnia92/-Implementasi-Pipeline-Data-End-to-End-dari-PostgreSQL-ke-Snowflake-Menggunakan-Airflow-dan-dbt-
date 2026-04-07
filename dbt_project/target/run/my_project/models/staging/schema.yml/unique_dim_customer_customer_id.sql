select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from (select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_customer where row_status = 'CURRENT') dbt_subquery
where customer_id is not null
group by customer_id
having count(*) > 1



      
    ) dbt_internal_test