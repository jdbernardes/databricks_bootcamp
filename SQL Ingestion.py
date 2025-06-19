# Databricks notebook source
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the environment variable
user = os.environ['DB_USER'],
password = os.environ['DB_PASSWORD'],
driver = os.environ['DB_DRIVER']

# COMMAND ----------

##Table on postgres
postgresql_table = "transactions"

##Table on Databricks
databricks_table = "bronze_transactions"

transactions_df = (
  spark.read.format("jdbc")
  .option("url", os.environ['JDBC_URL'])
  .option("dbtable", postgresql_table)
  .option("user", os.environ['DB_USER'])
  .option("password", os.environ['DB_PASSWORD'])
  .option("driver", os.environ['DB_DRIVER'])
  .load()
  )

transactions_df.display()
transactions_df.write.mode("overwrite").saveAsTable(databricks_table)

# COMMAND ----------


