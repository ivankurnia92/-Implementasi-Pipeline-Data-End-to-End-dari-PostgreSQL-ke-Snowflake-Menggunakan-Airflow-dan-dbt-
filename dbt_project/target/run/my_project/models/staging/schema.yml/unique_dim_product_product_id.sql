select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    product_id as unique_field,
    count(*) as n_records

from (select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product where row_status = 'CURRENT') dbt_subquery
where product_id is not null
group by product_id
having count(*) > 1



      
    ) dbt_internal_test