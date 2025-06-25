# Databricks notebook source
# Nome da tabela no PostgreSQL e no Databricks
postgres_table = "transactions"
databricks_table = "bronze.transactions"

# 1. Obter o último `transaction_time` da tabela gerenciada no Databricks
last_transaction_time = spark.sql(f"""
    SELECT MAX(transaction_date) AS last_transaction_time 
    FROM {databricks_table}
""").collect()[0]["last_transaction_time"]

# Verificar se há um `last_transaction_time` válido
if last_transaction_time is None:
    print("Nenhum dado encontrado na tabela gerenciada. Carregando todos os dados do PostgreSQL.")
    query = f"(SELECT * FROM {postgres_table}) AS t"
else:
    print(f"Última transação encontrada: {last_transaction_time}. Carregando apenas os dados novos.")
    query = f"""
        (SELECT * 
         FROM {postgres_table} 
         WHERE transaction_date > '{last_transaction_time}') AS t
    """

# 2. Carregar apenas os dados novos do PostgreSQL para um DataFrame Spark
new_transactions_df = (
  spark.read.format("jdbc")
  .option("url", dbutils.secrets.get(scope="databricks_bootcamp", key="JDBC_URL"))
  .option("dbtable", query)
  .option("user", dbutils.secrets.get(scope="databricks_bootcamp", key="DB_USER"))
  .option("password", dbutils.secrets.get(scope="databricks_bootcamp", key="DB_PASSWORD"))
  .option("driver", dbutils.secrets.get(scope="databricks_bootcamp", key="DB_DRIVER"))
  .load()
)

# Verificar se há dados novos para inserir
if new_transactions_df.count() > 0:
    print(f"Inserindo {new_transactions_df.count()} novos registros na tabela gerenciada.")
    
    # 3. Inserir os dados novos na tabela gerenciada
    new_transactions_df.write.format("delta").mode("append").saveAsTable(databricks_table)
    
    print("Novos dados inseridos com sucesso!")
else:
    print("Nenhum dado novo encontrado para inserção.")

# COMMAND ----------


