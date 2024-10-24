-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS `main`.`billing_forecast`;
CREATE TABLE IF NOT EXISTS `main`.`billing_forecast`.billing_forecast (sku STRING, workspace_id STRING);
INSERT INTO `main`.`billing_forecast`.billing_forecast (sku, workspace_id) SELECT
    'ALL' AS a,
    'ALL' AS b
WHERE NOT EXISTS (SELECT * FROM `main`.`billing_forecast`.billing_forecast);

-- COMMAND ----------

SELECT DISTINCT workspace_id FROM `main`.`billing_forecast`.billing_forecast
-- temporary workaround to remove cache due to the way dbdemos import query - will be removed in lakeview
WHERE NOW() > '2000-01-01'
ORDER BY workspace_id DESC;

-- COMMAND ----------

SELECT
    gender,
    COUNT(gender) AS total_churn
FROM `main`.`dbdemos_retail_c360`.churn_features
WHERE churn = 1
GROUP BY gender
