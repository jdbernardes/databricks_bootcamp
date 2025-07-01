# Databricks notebook source
# MAGIC %sql
# MAGIC MERGE INTO gold.customers AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         c.customer_id,
# MAGIC         c.name,
# MAGIC         c.email,
# MAGIC         c.usd_balance_original,  -- Mantendo saldo original de USD
# MAGIC         c.btc_balance_original,  -- Mantendo saldo original de BTC
# MAGIC         -- Calculando saldo atualizado de BTC considerando compras e vendas
# MAGIC         c.btc_balance_original + COALESCE(SUM(
# MAGIC             CASE 
# MAGIC                 WHEN t.transaction_type = 'compra' THEN t.btc_amount  -- Se comprou, adiciona BTC
# MAGIC                 WHEN t.transaction_type = 'venda' THEN -t.btc_amount  -- Se vendeu, reduz BTC
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ), 0) AS btc_balance_final,
# MAGIC         COUNT(t.transaction_id) AS total_transactions,
# MAGIC         -- Calculando total de USD gasto somando transaction_value_in_usd com usd_balance_original
# MAGIC         c.usd_balance_original + COALESCE(SUM(t.transaction_value_in_usd), 0) AS usd_balance_final
# MAGIC     FROM silver.customers c
# MAGIC     LEFT JOIN gold.transaction t ON c.customer_id = t.customer_id
# MAGIC     GROUP BY c.customer_id, c.name, c.email, c.usd_balance_original, c.btc_balance_original
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.email = source.email,
# MAGIC     target.usd_balance_original = source.usd_balance_original,
# MAGIC     target.btc_balance_original = source.btc_balance_original,
# MAGIC     target.btc_balance_final = source.btc_balance_final,
# MAGIC     target.total_transactions = source.total_transactions,
# MAGIC     target.usd_balance_final = source.usd_balance_final
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (customer_id, name, email, usd_balance_original, btc_balance_original, btc_balance_final, total_transactions, usd_balance_final)
# MAGIC   VALUES (source.customer_id, source.name, source.email, source.usd_balance_original, source.btc_balance_original, source.btc_balance_final, source.total_transactions, source.usd_balance_final);

# COMMAND ----------


