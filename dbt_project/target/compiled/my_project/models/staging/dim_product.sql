

with raw_snapshot as (
    /* Mengambil data dari snapshots/product_snapshot.sql */
    select * from DBT_DEV_DB.SNAPSHOTS.product_snapshot
),

deduplicated as (
    -- Melakukan deduplikasi berdasarkan ID asli dari snapshot
    select 
        *,
        row_number() over (
            partition by ID 
            order by dbt_updated_at desc
        ) as row_num
    from raw_snapshot
),

final as (
    select
        -- 1. Business Keys
        ID as PRODUCT_ID,
        PRODUCT_CODE,
        PRODUCT_NAME,
        DESCRIPTION,
        
        -- 2. Supplier & Category Info
        SUPPLIER_IDS, 
        CATEGORY,
        
        -- 3. Pricing & Inventory Info
        STANDARD_COST,
        LIST_PRICE,
        REORDER_LEVEL,
        TARGET_LEVEL,
        QUANTITY_PER_UNIT,
        
        -- 4. Status Produk (Discontinued Logic)
        case 
            when DISCONTINUED = 1 then 'Discontinued'
            else 'Active'
        end as DISCONTINUED_STATUS,
        
        -- 5. Metadata SCD Tipe 2
        DBT_VALID_FROM as EFFECTIVE_START_DATE,
        DBT_VALID_TO as EFFECTIVE_END_DATE,
        
        -- 6. Flag Baris Aktif (SCD Type 2 Logic)
        case 
            when row_num = 1 and DBT_VALID_TO is null then 'CURRENT'
            else 'HISTORICAL'
        end as ROW_STATUS

    from deduplicated
)

select * from final