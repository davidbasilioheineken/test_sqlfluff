-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS as
SELECT
    c1 COL1,
    COL2,col3,
    col11
FROM (SELECT DISTINCT *  FROM LOGS);
