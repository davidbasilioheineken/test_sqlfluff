-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS `main`.`billing_forecast`;
CREATE TABLE IF NOT EXISTS `main`.`billing_forecast`.billing_forecast (sku STRING, workspace_id STRING);
INSERT INTO `main`.`billing_forecast`.billing_forecast (sku, workspace_id) SELECT 'ALL', 'ALL'
  WHERE NOT EXISTS (SELECT * FROM  `main`.`billing_forecast`.billing_forecast);

-- COMMAND ----------

select distinct(workspace_id) from `main`.`billing_forecast`.billing_forecast 
  where now() > '2000-01-01' -- temporary workaround to remove cache due to the way dbdemos import query - will be removed in lakeview
order by workspace_id DESC;

-- COMMAND ----------

SELECT gender, count(gender) as total_churn FROM `main`.`dbdemos_retail_c360`.churn_features where churn = 1 GROUP BY gender

