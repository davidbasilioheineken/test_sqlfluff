-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS AS
SELECT
    COLUMN1 as COL1,
    col2,
    COL3, col4
FROM(SELECT DISTINCT * FROM LOGS);
