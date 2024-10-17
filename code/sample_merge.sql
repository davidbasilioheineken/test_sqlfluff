-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
SELECT
    column1,    col2,
    col3
    ,COL4
from(SELECT DISTINCT * FROM logs);
