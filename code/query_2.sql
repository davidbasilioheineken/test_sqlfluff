-- Databricks notebook source
SELECT
    p.country AS mex,
    p.churn AS c,
    COUNT(*) AS customers
FROM `main`.`dbdemos_retail_c360`.churn_features AS p
GROUP BY p.country, p.churn

-- COMMAND ----------

SELECT
    c.canal,
    SUM(u.amount) / 100 AS mrr
FROM `main`.`dbdemos_retail_c360`.churn_orders AS c

INNER JOIN `main`.`dbdemos_retail_c360`.churn_users AS u ON c.user_id = u.user_id
GROUP BY c.canal;

-- COMMAND ----------

WITH customer_total_return AS (
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    WHERE
        sr_returned_date_sk = d_date_sk
        AND d_year = 2000
    GROUP BY
        sr_customer_sk,
        sr_store_sk
)

SELECT
    ctr1.ctr_customer_sk,
    ctr1.ctr_store_sk,
    ctr1.ctr_total_return
FROM customer_total_return AS ctr1,
    store AS s,
    customer AS c
WHERE
    ctr1.ctr_total_return > (
        SELECT AVG(ctr1.ctr_total_return) * 1.2
        FROM customer_total_return AS ctr2
        WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
    AND s.store_sk = ctr1.ctr_store_sk
    AND s.state = 'TN'
    AND ctr1.ctr_customer_sk = c.customer_sk
ORDER BY c.customer_id
LIMIT 100;
