select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    employee_id as unique_field,
    count(*) as n_records

from (select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_employee where row_status = 'CURRENT') dbt_subquery
where employee_id is not null
group by employee_id
having count(*) > 1



      
    ) dbt_internal_test