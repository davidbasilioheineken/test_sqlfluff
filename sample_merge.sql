-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
select column1
FROM (SELECT DISTINCT * FROM TBL);
