-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
SELECT column1
FROM (SELECT DISTINCT * FROM tbl);
