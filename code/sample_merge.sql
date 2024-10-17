-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
SELECT
    column1,
    col2,
    col3,
    col4
FROM (SELECT DISTINCT * FROM logs);
