# Databricks notebook source
# MAGIC %md
# MAGIC ## Insert Overwrite on Customers

# COMMAND ----------

# MAGIC %md
# MAGIC In this approach I am recreating my table customers everytime the job runs for this table.
# MAGIC This approach makes sense here because my table customers (for this scenario) is finite and is not a big table but depending on the size of the table it may start getting expensive

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view customers_view as
# MAGIC select 
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   cast (usd_balance as double) as usd_balance_original,
# MAGIC   btc_balance as btc_balance_original
# MAGIC from bronze.customers
# MAGIC where name not in ('Mark Cunningham', 'Mark Savage')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert overwrite silver.customers
# MAGIC select * from customers_view
