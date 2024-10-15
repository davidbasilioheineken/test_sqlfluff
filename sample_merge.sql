-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
select
    column1
    ,col2,
    col3
from (select distinct * from logs);

-- COMMAND ----------

-- Create a temporary view with distinct rows and enriched data from user_info
CREATE TEMPORARY VIEW NEW_DEDUPED_LOGS_VIEW AS
select *
from (select distinct * from dedupedLogs)
join user_info using(user_id);

-- Perform the MERGE operation
merge into logs using new_deduped_logs_view on logs.uniqueid = new_deduped_logs_view.uniqueid when not matched then insert *;
