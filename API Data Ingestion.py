# Databricks notebook source
import requests
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from datetime import datetime

url = dbutils.secrets.get(scope="databricks_bootcamp", key="API_URL")

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
            "amount": data['amount'],
            "base": data['base'],
            "currency": data['currency'],
            "datetime": datetime.now()
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    # Get API Data
    print("Getting API Data...")
    bitcoin_price = fetch_bitcoin_price()
    print("Data fetched successfully!")
    print(bitcoin_price)

# COMMAND ----------


