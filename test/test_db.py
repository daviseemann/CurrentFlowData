import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

print(f"User: {os.getenv('POSTGRES_USER')}")
print(f"Password: {os.getenv('POSTGRES_PASSWORD')}")
print(f"Database: {os.getenv('POSTGRES_DB')}")

try:
    connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
    )
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"Conectado ao PostgreSQL, versão: {db_version}")

except (Exception, psycopg2.Error) as error:
    print("Erro ao conectar ao banco de dados PostgreSQL:", error)

finally:
    if 'connection' in locals() and connection:
        cursor.close()
        connection.close()
        print("Conexão com PostgreSQL fechada")
