-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS AS
select
    col COL1,
    COL2,    COL3,COL4
FROM(SELECT DISTINCT * FROM LOGS);
