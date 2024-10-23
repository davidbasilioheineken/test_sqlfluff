-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS AS
SELECT
    col COL1,
    COL2,
    col3,
    COL11
FROM (SELECT DISTINCT * FROM LOGS);
