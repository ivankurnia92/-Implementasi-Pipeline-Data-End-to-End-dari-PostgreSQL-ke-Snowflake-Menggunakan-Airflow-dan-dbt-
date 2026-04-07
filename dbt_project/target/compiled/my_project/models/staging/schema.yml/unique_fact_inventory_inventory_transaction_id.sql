
    
    

select
    inventory_transaction_id as unique_field,
    count(*) as n_records

from DBT_DEV_DB.PUBLIC_2_STAGING.fact_inventory
where inventory_transaction_id is not null
group by inventory_transaction_id
having count(*) > 1


