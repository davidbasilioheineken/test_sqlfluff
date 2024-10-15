-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs  AS
SELECT *
FROM (SELECT DISTINCT * FROM tbl)
JOIN user_info USING(user_id);
