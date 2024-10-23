-- Databricks notebook source
CREATE or REFRESH STREAMING TABLE tbl_stg_mae
(
    CONSTRAINT valid_records
    EXPECT (
        num_cliente IS NOT NULL
        and nombre_cliente IS NOT NULL
    )
    on VIOLATION DROP ROW
)
COMMENT "This table contains all valid records for MAE"
    
    TBLPROPERTIES
    (
        "quality" = "silver",
        "delta.enableChangeDataFeed" = "true",
        "department" = "Sales Growth Planning",
        "source" = "SAP"
    );

-- COMMAND ----------

APPLY CHANGES INTO live.tbl_stg_mae
FROM STREAM(heiaepmx001dwe01.heiaepmxddb_ing.tbl_ing_mae)
KEYS(num_cliente,
org_ventas)
SEQUENCE  by processed_date
STORED AS SCD TYPE   2;
