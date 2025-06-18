# Databricks notebook source
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the environment variable
my_variable = os.getenv('TEST')
user = os.environ['DB_USER'],
password = os.environ['DB_PASSWORD'],
driver = os.environ['DB_DRIVER']

# COMMAND ----------

jdbc_url = "jdbc:postgresql://dpg-d181ieggjchc73f9h2bg-a.oregon-postgres.render.com:5432/transactions_3fnf"
connectionProperties = {
  "user" : os.environ['DB_USER'],
  "password" : os.environ['DB_PASSWORD'],
  "driver" : os.environ['DB_DRIVER']
}

##Table on postgres
postgresql_table = "transactions"

##Table on Databricks
databricks_table = "bronze_transactions"

transactions_df = (
  spark.read.format("jdbc")
  .option("url", jdbc_url)
  .option("dbtable", postgresql_table)
  .option("user", os.environ['DB_USER'])
  .option("password", os.environ['DB_PASSWORD'])
  .option("driver", os.environ['DB_DRIVER'])
  .load()
  )

transactions_df.display()
# df = spark.read.jdbc(url=jdbc_url, table="transactions", properties=connectionProperties)

# COMMAND ----------

transactions_df.write.mode("overwrite").saveAsTable(databricks_table)
