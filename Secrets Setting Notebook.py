# Databricks notebook source
# MAGIC %md
# MAGIC In this notebook we presume that you'll have the following values which must be added in the secrets
# MAGIC
# MAGIC - DB_USER = Your DB user
# MAGIC - DB_PASSWORD = Your DB Password
# MAGIC - DB_DRIVER = 'org.postgresql.Driver'
# MAGIC - JDBC_URL = The jdbc url starting with"jdbc:postgresql://[DB_HOST]:[DB_PORT]/[DB_NAME]"
# MAGIC - DB_HOST = Your DB host wherever this is
# MAGIC - DB_NAME = Name of your DB
# MAGIC - DATABRICKS_INSTANCE = Your databricks instance
# MAGIC - DATABRICKS_TOKEN = Your databricks access token
# MAGIC - API_URL = Any API url that you want to use for learning I'll use BTC API

# COMMAND ----------

!pip install python-dotenv --quiet

# COMMAND ----------

from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

# COMMAND ----------

# Define the necessary variables
databricks_instance = f"https://{os.environ['DATABRICKS_INSTANCE']}"
token = os.environ['DATABRICKS_TOKEN']
scope_name = "databricks_bootcamp"

# Set up the headers and payload
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
payload = {
    "scope": scope_name
}

# Make the API request to create the secret scope
response = requests.post(
    f"{databricks_instance}/api/2.0/secrets/scopes/create",
    headers=headers,
    data=json.dumps(payload)
)

# Check the response
if response.status_code == 200:
    print("Secret scope created successfully.")
else:
    print(f"Failed to create secret scope: {response.text}")

# COMMAND ----------

# Define the necessary variables
databricks_instance = f"https://{os.environ['DATABRICKS_INSTANCE']}"
token = os.environ['DATABRICKS_TOKEN']
scope_name = "databricks_bootcamp"

# Set up the headers
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Define the secrets to add
secrets = {
    "DB_USER": os.environ['DB_USER'],
    "DB_PASSWORD": os.environ['DB_PASSWORD'],
    "DB_DRIVER": os.environ['DB_DRIVER'],
    "JDBC_URL": os.environ['JDBC_URL'],
    "API_URL": os.environ['API_URL'],
    "PUBLIC_S3": os.environ['PUBLIC_S3']
}

# Add each secret to the scope
for key, value in secrets.items():
    payload = {
        "scope": scope_name,
        "key": key,
        "string_value": value
    }
    response = requests.post(
        f"{databricks_instance}/api/2.0/secrets/put",
        headers=headers,
        data=json.dumps(payload)
    )
    if response.status_code == 200:
        print(f"Secret {key} added successfully.")
    else:
        print(f"Failed to add secret {key}: {response.text}")

# COMMAND ----------


