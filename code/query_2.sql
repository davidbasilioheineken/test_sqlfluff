-- Databricks notebook source
SELECT
    p.country AS mex,
    p.churn AS c,
    COUNT(*) AS customers
FROM `main`.`dbdemos_retail_c360`.churn_features AS p
GROUP BY p.country, p.churn

-- COMMAND ----------

SELECT
    canal,
    SUM(amount) / 100 AS mrr
FROM `main`.`dbdemos_retail_c360`.churn_orders
INNER JOIN `main`.`dbdemos_retail_c360`.churn_users ON churn_orders.user_id = churn_users.user_id
GROUP BY canal;

-- COMMAND ----------

WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns,
        date_dim
    WHERE
        sr_returned_date_sk = d_date_sk
        AND d_year = 2000
    GROUP BY
        sr_customer_sk,
        sr_store_sk
)

SELECT *
FROM customer_total_return AS ctr1,
    store,
    customer
WHERE
    ctr1.ctr_total_return > (
        SELECT AVG(ctr_total_return) * 1.2
        FROM customer_total_return AS ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
    AND s_store_sk = ctr1.ctr_store_sk
    AND s_state = 'TN'
    AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100;
