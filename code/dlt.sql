-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE tbl_mae (
    CONSTRAINT valid_records
    EXPECT (
        num_cliente IS NOT NULL
        AND nombre_cliente IS NOT NULL
    )
    ON VIOLATION DROP ROW
)
COMMENT "this table contains all valid records for mae"

    TBLPROPERTIES (
        "quality" = "silver",
        "delta.enablechangedatafeed" = "true",
        "department" = "sales growth planning",
        "source" = "sap"
    );

-- COMMAND ----------

APPLY CHANGES INTO live.tbl_mae
FROM STREAM(heiaepmx001dwe01.heiaepmxddb_ing.tbl_mae_ing)
KEYS (
    num_cliente, org_ventas
)
SEQUENCE BY processed_date
STORED AS SCD TYPE 2;
