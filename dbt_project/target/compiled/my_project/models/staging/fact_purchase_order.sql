

with po_headers as (
    -- Gunakan SELECT DISTINCT jika ada indikasi header terduplikasi di source
    select * from "DBT_DEV_DB"."PUBLIC_1_RAW"."PURCHASE_ORDERS"
),

po_details as (
    -- Detail transaksi biasanya memiliki ID unik sendiri
    select * from "DBT_DEV_DB"."PUBLIC_1_RAW"."PURCHASE_ORDER_DETAILS"
),

dim_product as (
    -- Memastikan hanya mengambil data produk yang aktif
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_product where ROW_STATUS = 'CURRENT'
),

dim_employee as (
    -- Memastikan hanya mengambil data karyawan yang aktif
    select * from DBT_DEV_DB.PUBLIC_2_STAGING.dim_employee where ROW_STATUS = 'CURRENT'
)

select
    -- 1. Keys (Primary & Foreign Keys)
    -- Pastikan kolom ID di pod adalah unik untuk baris detail ini
    pod.ID as PO_DETAIL_ID,
    po.ID as PURCHASE_ORDER_ID,
    p.PRODUCT_ID,
    e.EMPLOYEE_ID,
    po.SUPPLIER_ID,
    
    -- 2. Metrics (Kalkulasi Finansial)
    pod.QUANTITY,
    pod.UNIT_COST,
    -- Menghitung total biaya per baris
    round((pod.QUANTITY * pod.UNIT_COST), 2) as LINE_TOTAL_COST,
    
    -- 3. Dimension & Date Info
    cast(po.CREATION_DATE as date) as CREATION_DATE,
    po.EXPECTED_DATE,
    pod.DATE_RECEIVED,
    
    -- 4. Status & Payment info
    po.STATUS_ID,
    po.PAYMENT_METHOD,
    
    -- 5. Audit Metadata
    current_timestamp() as INSERTION_TIMESTAMP

from po_headers po
inner join po_details pod 
    on po.ID = pod.PURCHASE_ORDER_ID
left join dim_product p 
    on pod.PRODUCT_ID = p.PRODUCT_ID
left join dim_employee e 
    -- Pastikan kolom di po_headers bernama CREATED_BY
    on po.CREATED_BY = e.EMPLOYEE_ID