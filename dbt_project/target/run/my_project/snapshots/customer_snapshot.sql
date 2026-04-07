
      begin;
    merge into "DBT_DEV_DB"."SNAPSHOTS"."CUSTOMER_SNAPSHOT" as DBT_INTERNAL_DEST
    using "DBT_DEV_DB"."SNAPSHOTS"."CUSTOMER_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("ID", "COMPANY", "LAST_NAME", "FIRST_NAME", "JOB_TITLE", "BUSINESS_PHONE", "ADDRESS", "CITY", "STATE_PROVINCE", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("ID", "COMPANY", "LAST_NAME", "FIRST_NAME", "JOB_TITLE", "BUSINESS_PHONE", "ADDRESS", "CITY", "STATE_PROVINCE", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  