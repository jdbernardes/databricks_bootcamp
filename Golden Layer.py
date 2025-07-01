# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS transaction_gold;
# MAGIC CREATE TEMPORARY VIEW transaction_gold_view AS
# MAGIC WITH btc_price AS (
# MAGIC     SELECT 
# MAGIC         datetime, 
# MAGIC         amount,
# MAGIC         LAG(datetime) OVER (ORDER BY datetime) AS prev_timestamp
# MAGIC     FROM silver.bitcoin_price
# MAGIC )
# MAGIC SELECT 
# MAGIC     t.transaction_id,
# MAGIC     t.customer_id,
# MAGIC     t.btc_amount,
# MAGIC     CAST(t.transaction_type AS STRING) AS transaction_type,  -- Garantindo que transaction_type seja STRING
# MAGIC     -- Valor da transação em USD (compra negativa, venda positiva)
# MAGIC     ROUND(
# MAGIC         CASE 
# MAGIC             WHEN t.transaction_type = 'compra' THEN (t.btc_amount * p.amount) * -1  
# MAGIC             ELSE (t.btc_amount * p.amount)  
# MAGIC         END, 2
# MAGIC     ) AS transaction_value_in_usd,
# MAGIC     t.transaction_date
# MAGIC FROM silver.transactions t
# MAGIC JOIN btc_price p
# MAGIC     ON t.transaction_date >= COALESCE(p.prev_timestamp, p.datetime)  -- Garantindo que prev_timestamp nunca seja NULL
# MAGIC     AND t.transaction_date <= p.datetime;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table gold.transaction as
# MAGIC select * from transaction_gold_view;
# MAGIC
# MAGIC select * from gold.transaction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW customers_gold_view AS
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     c.name,
# MAGIC     c.email,
# MAGIC     c.usd_balance_original,  -- Mantendo saldo original de USD
# MAGIC     c.btc_balance_original,  -- Mantendo saldo original de BTC
# MAGIC     -- Calculando saldo atualizado de BTC considerando compras e vendas
# MAGIC     c.btc_balance_original + COALESCE(SUM(
# MAGIC         CASE 
# MAGIC             WHEN t.transaction_type = 'compra' THEN t.btc_amount  -- Se comprou, adiciona BTC
# MAGIC             WHEN t.transaction_type = 'venda' THEN -t.btc_amount  -- Se vendeu, reduz BTC
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ), 0) AS btc_balance_final,
# MAGIC     COUNT(t.transaction_id) AS total_transactions,
# MAGIC     -- Calculando total de USD gasto somando transaction_value_in_usd com usd_balance_original
# MAGIC     c.usd_balance_original + COALESCE(SUM(t.transaction_value_in_usd), 0) AS usd_balance_final
# MAGIC FROM silver.customers c
# MAGIC LEFT JOIN gold.transaction t ON c.customer_id = t.customer_id
# MAGIC GROUP BY c.customer_id, c.name, c.email, c.usd_balance_original, c.btc_balance_original;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.customers AS 
# MAGIC select * from customers_gold_view;
# MAGIC
# MAGIC select * from gold.customers

# COMMAND ----------


