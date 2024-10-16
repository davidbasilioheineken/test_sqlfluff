-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE tbl_stg_mae 
(
  CONSTRAINT valid_records 
    EXPECT (
            NUM_CLIENTE IS NOT NULL and 
            NOMBRE_CLIENTE IS NOT NULL
            ) 
    ON VIOLATION DROP ROW
)
COMMENT "This table contains all valid records for MAE"
TBLPROPERTIES 
(
  "quality" = "silver", 
  "delta.enableChangeDataFeed" = "true", 
  "department" = "Sales Growth Planning", 
  "source" = "SAP"
);

APPLY CHANGES INTO LIVE.tbl_stg_mae 
FROM STREAM(heiaepmx001dwe01.heiaepmxddb_ing.tbl_ing_mae)
  KEYS (NUM_CLIENTE, ORG_VENTAS)
  SEQUENCE BY processed_date
     STORED AS SCD TYPE 2;
