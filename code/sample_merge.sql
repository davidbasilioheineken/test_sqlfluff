-- Databricks notebook source
CREATE TEMPORARY VIEW NEW_LOGS as
select
    column1 col1 ,
    COL2,
    col3 ,col4
from(select distinct * FROM logs);
