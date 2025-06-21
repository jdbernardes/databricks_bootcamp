# Databricks notebook source
# %sql
# -- Tabela de origem: Lê os arquivos JSON do bucket S3
# CREATE OR REFRESH STREAMING LIVE TABLE bronze_customers
# COMMENT "Dados brutos dos novos clientes do bucket S3"
# AS SELECT *
# FROM cloud_files(
#     "s3a://databricks-bootcamp-julio",
#     "json",
#     MAP(
#         "inferSchema", "false",
#         "multiline", "true",
#         "cloudFiles.schemaHints", "customer_id STRING, name STRING, email STRING, btc_balance DOUBLE, usd_balance DOUBLE, last_update TIMESTAMP",
#         "cloudFiles.schemaLocation", "/FileStore/autoloader_schemas/bronze_customers"
#     )
# );

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

s3_bucket_path = dbutils.secrets.get(scope="databricks_bootcamp", key="PUBLIC_S3")

# Defining my schema
customer_schema = StructType([
    StructField("customer_id", StringType(), True),   # customer_id como String
    StructField("name", StringType(), True),          # name como String
    StructField("email", StringType(), True),         # email como String
    StructField("btc_balance", DoubleType(), True),   # btc_balance como Double
    StructField("usd_balance", DoubleType(), True),   # usd_balance como Double
    StructField("last_update", TimestampType(), True) # last_update como Timestamp
])

print(f"Lendo arquivos JSON do caminho S3: {s3_bucket_path}")
try:
    df_customers = spark.read \
        .format("json") \
        .option("multiline", "true") \
        .schema(customer_schema) \
        .load(s3_bucket_path)

    print("Pré-visualização dos dados lidos:")
    df_customers.show(5, truncate=False)
    df_customers.printSchema()

    # Saving Data
    table_name = "bronze.customers"
    print(f"Saving data on Delta table: {table_name}")

    df_customers.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    print(f"Data Saved'{table_name}'.")

    # 4. Verificando a tabela Delta criada
    print(f"Checking Data on the Table'{table_name}':")
    display(spark.sql(f"SELECT * FROM {table_name} LIMIT 10"))

except Exception as e:
    print(f"Ocorreu um erro ao processar os dados: {e}")
