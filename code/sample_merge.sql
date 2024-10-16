-- Databricks notebook source
CREATE TEMPORARY VIEW new_logs AS
select
    column1
    ,col2,
    col3
from (select distinct * from logs);
