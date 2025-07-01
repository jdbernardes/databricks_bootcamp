# Databricks notebook source
# MAGIC %sql
# MAGIC MERGE INTO gold.transaction AS g
# MAGIC USING (
# MAGIC     WITH btc_price AS (
# MAGIC         SELECT 
# MAGIC             datetime, 
# MAGIC             amount,
# MAGIC             LAG(datetime) OVER (ORDER BY datetime) AS prev_timestamp
# MAGIC         FROM silver.bitcoin_price
# MAGIC     )
# MAGIC     SELECT 
# MAGIC         t.transaction_id,
# MAGIC         t.customer_id,
# MAGIC         t.transaction_type,
# MAGIC         t.btc_amount,
# MAGIC         t.transaction_date,
# MAGIC         ROUND(
# MAGIC             CASE 
# MAGIC                 WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.amount) * -1  
# MAGIC                 ELSE (t.btc_amount * p.amount)  
# MAGIC             END, 2
# MAGIC         ) AS transaction_value_in_usd
# MAGIC     FROM silver.transactions t
# MAGIC     JOIN btc_price p
# MAGIC         ON t.transaction_date >= COALESCE(p.prev_timestamp, p.datetime)
# MAGIC         AND t.transaction_date <= p.datetime
# MAGIC ) AS s
# MAGIC ON g.transaction_id = s.transaction_id
# MAGIC WHEN MATCHED AND (
# MAGIC     g.customer_id <> s.customer_id OR
# MAGIC     g.transaction_type <> s.transaction_type OR
# MAGIC     g.btc_amount <> s.btc_amount OR
# MAGIC     g.transaction_date <> s.transaction_date OR
# MAGIC     g.transaction_value_in_usd <> s.transaction_value_in_usd
# MAGIC )
# MAGIC THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------


