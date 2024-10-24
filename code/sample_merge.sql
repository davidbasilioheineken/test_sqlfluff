-- Databricks notebook source
create temporary view new_logs as
select
    co col1,
    col2,
    col3,
    col11
from (select * from logs);
