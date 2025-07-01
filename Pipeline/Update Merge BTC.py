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
# MAGIC MERGE INTO silver.bitcoin_price AS target
# MAGIC USING bronze.bitcoin_price AS source
# MAGIC ON target.datetime = source.datetime
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (amount, datetime)
# MAGIC   VALUES (source.amount, source.datetime)
