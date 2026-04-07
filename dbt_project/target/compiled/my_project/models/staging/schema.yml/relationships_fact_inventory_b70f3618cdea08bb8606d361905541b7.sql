
    
    

with child as (
    select product_id as from_field
    from DBT_DEV_DB.PUBLIC_2_STAGING.fact_inventory
    where product_id is not null
),

parent as (
    select product_id as to_field
    from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


