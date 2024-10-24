-- Databricks notebook source
create or refresh streaming table tbl_stg_mae (
    constraint valid_records
    expect (
        num_cliente is not null
        and nombre_cliente is not null
    )
    on violation drop row
)
comment "this table contains all valid records for mae"

tblproperties (
    "quality" = "silver",
    "delta.enablechangedatafeed" = "true",
    "department" = "sales growth planning",
    "source" = "sap"
);

-- COMMAND ----------

APPLY CHANGES INTO live.TBL_MAE
FROM STREAM(heiaepmx001dwe01.heiaepmxddb_ing.TBL_MAE_ING)
KEYS (
    num_cliente,org_ventas
)
SEQUENCE BY processed_date
STORED AS SCD  TYPE 2;
