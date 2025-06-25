# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customers

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

# MAGIC  %sql
# MAGIC  select * from customers_view

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Here I am creating the table on silver layer out of my temporary view
# MAGIC */
# MAGIC
# MAGIC create table silver.customers as
# MAGIC select * from customers_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS transactions_view;
# MAGIC create temporary view transactions_view as
# MAGIC select 
# MAGIC   transaction_id,
# MAGIC   customer_id,
# MAGIC   transaction_type,
# MAGIC   btc_amount,
# MAGIC   transaction_date
# MAGIC from bronze.transactions
# MAGIC where transaction_date > '2025-06-24' and transaction_date < '2025-06-25';

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.transactions as
# MAGIC select * from transactions_view 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.transactions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver BTC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.bitcoin_price

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS bitcoin_price_view;
# MAGIC create temporary view bitcoin_price_view as
# MAGIC select 
# MAGIC amount,
# MAGIC datetime 
# MAGIC from bronze.bitcoin_price;
# MAGIC
# MAGIC select* from bitcoin_price_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.bitcoin_price as
# MAGIC select * from bitcoin_price_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.bitcoin_price
