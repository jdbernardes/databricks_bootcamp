# Databricks notebook source
!pip install faker psycopg2-binary python-dotenv --quiet

# COMMAND ----------

import random
from faker import Faker
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Inicializar Faker
fake = Faker()

# Configurações de Data
start_date = datetime(2025, 1, 29)
end_date = datetime(2025, 1, 31)
transactions_per_day = 100  # Transações por dia
peak_hours_start = 18  # 6 PM
peak_hours_end = 21  # 9 PM

# Configuração de Conexão ao Banco de Dados PostgreSQL
db_config = {
    "dbname": os.environ['DB_NAME'],
    "user": os.environ['DB_USER'],
    "password": os.environ['DB_PASSWORD'],
    "host": os.environ['DB_HOST'],
    "port": 5432
}

# Função para gerar dados para a tabela `transactions`
def generate_transactions(transactions_per_day, customer_ids):
    transactions = []
    current_date = start_date
    high_transaction_days = {datetime(2025, 1, 5), datetime(2025, 1, 15)}  # Dias com mais transações
    high_transaction_customers = random.sample(customer_ids, 3)  # Clientes com mais transações

    while current_date <= end_date:
        daily_transactions = transactions_per_day * 2 if current_date in high_transaction_days else transactions_per_day

        for _ in range(daily_transactions):
            transaction_type = random.choice(["compra", "venda"])
            btc_amount = round(random.uniform(0.01, 2), 6)
            usd_value = round(btc_amount * random.uniform(30000, 50000), 2)

            # Definir horário da transação com picos entre 6 PM e 9 PM
            if random.random() < 0.7:  # 70% no horário de pico
                random_time = fake.date_time_between_dates(
                    datetime_start=current_date + timedelta(hours=peak_hours_start),
                    datetime_end=current_date + timedelta(hours=peak_hours_end)
                )
            else:  # Fora do pico
                random_time = fake.date_time_between_dates(
                    datetime_start=current_date,
                    datetime_end=current_date + timedelta(hours=23, minutes=59)
                )

            # Aumentar a chance de alguns clientes terem mais transações
            customer_id = random.choice(high_transaction_customers) if random.random() < 0.6 else random.choice(customer_ids)

            transactions.append({
                "transaction_id": fake.uuid4(),
                "customer_id": customer_id,
                "transaction_type": transaction_type,
                "btc_amount": btc_amount,
                "usd_value": usd_value,
                "transaction_date": random_time.strftime('%Y-%m-%d %H:%M:%S')
            })

        current_date += timedelta(days=1)
    return transactions

# Função para inserir dados em lotes no PostgreSQL
def insert_into_postgres_batch(data, table_name, db_config, batch_size=1000):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Inserir dados em lotes
        batch = []
        for i, row in enumerate(data):
            batch.append((
                row["transaction_id"],
                row["customer_id"],
                row["transaction_type"],
                row["btc_amount"],
                row["usd_value"],
                row["transaction_date"]
            ))

            # Executar inserção quando atingir o tamanho do lote
            if (i + 1) % batch_size == 0 or i == len(data) - 1:
                cursor.executemany(f"""
                    INSERT INTO {table_name} (transaction_id, customer_id, transaction_type, btc_amount, usd_value, transaction_date)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch = []  # Resetar o lote após inserção

        print(f"Dados inseridos na tabela `{table_name}` com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    customer_wallets_file = "/Volumes/workspace/default/customer_wallets/customer_wallets.csv"  # Path to the CSV file
    transactions_table = "transactions"  # Name of the table in PostgreSQL

    # Read the CSV file into a Spark DataFrame
    customer_wallets_df = spark.read.csv(customer_wallets_file, header=True, inferSchema=True)

    # Convert the DataFrame to a list of customer IDs
    customer_ids = [row.customer_id for row in customer_wallets_df.select("customer_id").collect()]

    # Generate transactions
    transactions = generate_transactions(transactions_per_day, customer_ids)

    # Insert transactions into PostgreSQL
    insert_into_postgres_batch(transactions, transactions_table, db_config)

# COMMAND ----------


