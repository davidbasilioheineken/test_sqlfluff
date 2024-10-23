-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS AS
SELECT
    COLUMN1 as COL1,
    col2,
    COL3, col4
from(SELECT DISTINCT * FROM LOGS);
