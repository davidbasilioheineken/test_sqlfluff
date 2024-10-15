-- Databricks notebook source
-- Create a temporary view with distinct rows and enriched data from user_info
CREATE TEMPORARY VIEW NEW_DEDUPED_LOGS_VIEW  AS
select *
from (select distinct * from tblLL)
join user_info using(user_id);
