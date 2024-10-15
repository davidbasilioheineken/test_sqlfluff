-- Databricks notebook source
-- Create a temporary view with distinct rows and enriched data from user_info
CREATE TEMPORARY VIEW NEW_DEDUPED_LOGS_VIEW  AS
SELECT *
FROM (SELECT DISTINCT * FROM tbl)
JOIN user_info USING(user_id);
