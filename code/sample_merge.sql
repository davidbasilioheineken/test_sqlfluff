-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS AS
SELECT
    C1 AS COL1,
    COL2,
    COL3,
    COL11
FROM (SELECT DISTINCT * FROM LOGS);
