-- Databricks notebook source
-- Create a temporary view with distinct rows and enriched data from user_info
CREATE TEMPORARY VIEW new_logs  AS
SELECT *
FROM (SELECT DISTINCT * FROM tbl)
JOIN user_info USING(user_id);
