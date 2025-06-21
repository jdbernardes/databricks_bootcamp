# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from datetime import datetime

url = dbutils.secrets.get(scope="databricks_bootcamp", key="API_URL")
table_name = "bronze.bitcoin_price"

def fetch_bitcoin_price():
    """Function created to get BTC price from API and return a Spark DataFrame with the following columns:
    - amount, base, currency, datetime
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()['data']
        # Adding timestamp to the data
        return{
            "amount": float(data['amount']),
            "base": data['base'],
            "currency": data['currency'],
            "datetime": datetime.now()
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def save_to_table(data):
    """Function created to save the data to a table in the database."""
    schema = StructType([
        StructField("amount", FloatType(), True),
        StructField("base", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("datetime", TimestampType(), True)
    ])
    data = spark.createDataFrame([data], schema = schema)
    data.write.format("delta").mode("append").saveAsTable(table_name)
    print("Data saved to table")


if __name__ == "__main__":
    # Get API Data
    print("Getting API Data and saving in Delta table")
    bitcoin_price = fetch_bitcoin_price()
    save_to_table(bitcoin_price)
    print("Data saved successfuly!")
    print(bitcoin_price)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.bitcoin_price

# COMMAND ----------


