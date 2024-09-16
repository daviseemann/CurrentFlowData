import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="admin123"
)

cursor = mydb.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS currentflow_db")

cursor.execute("""
CREATE TABLE IF NOT EXISTS currentflow_db.carga_di (
    id_subsistema INT,
    din_instante DATETIME,
    val_cargaenergiamwmed FLOAT,
    Ano INT
)
""")
