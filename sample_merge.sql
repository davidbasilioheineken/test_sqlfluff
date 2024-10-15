-- Databricks notebook source
-- Create a temporary view with distinct rows and enriched data from user_info
CREATE TEMPORARY VIEW NEW_DEDUPED_LOGS_VIEW  AS
select *
from (select distinct * from tbl)
join user_info using(user_id);

-- Perform the MERGE operation
merge into logs using new_deduped_logs_view on logs.uniqueid = new_deduped_logs_view.uniqueid when not matched then insert *;
