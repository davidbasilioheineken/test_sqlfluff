-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
SELECT
    co AS col1,
    col2,
    col3,
    col11
FROM (SELECT * FROM logs);
