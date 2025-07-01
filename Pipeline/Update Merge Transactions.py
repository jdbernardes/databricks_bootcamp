# Databricks notebook source
# MAGIC %md
# MAGIC ## Update Merge

# COMMAND ----------

# MAGIC %md
# MAGIC In the update Merge approach I am just adding the new records if any.
# MAGIC This can make sense if you have a big table with a lot of new records being created every day, this way you can just send the delta.
# MAGIC Notice that we use this "WHEN NOT MATCHED"

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     transaction_type,
# MAGIC     btc_amount,
# MAGIC     transaction_date
# MAGIC   FROM bronze.transactions
# MAGIC   WHERE transaction_date > '2025-06-25'
# MAGIC ) AS source
# MAGIC ON target.transaction_id = source.transaction_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (transaction_id, customer_id, transaction_type, btc_amount, transaction_date)
# MAGIC   VALUES (source.transaction_id, source.customer_id, source.transaction_type, source.btc_amount, source.transaction_date)
