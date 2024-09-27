import psycopg2
import logging
import pandas as pd


# Function to connect to the PostgreSQL database
def db_connect():
    try:
        conn = psycopg2.connect(
            dbname="current_flow_db",
            user="postgres",
            password="admin321",
            host="localhost",
            port="5432",
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error("Unable to connect to the database.")
        logging.error(e)
        return None


# Function to create tables if they don't exist
def create_tables(cursor):
    try:
        logging.info("Creating tables if they don't exist.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS carga_diaria (
                id SERIAL PRIMARY KEY,
                id_subsistema VARCHAR NOT NULL,
                nom_subsistema VARCHAR NOT NULL,
                din_instante VARCHAR NOT NULL,
                val_cargaenergiamwmed VARCHAR,
                Ano INTEGER NOT NULL,
                input_file VARCHAR NOT NULL,
                CONSTRAINT unique_constraint_carga_diaria UNIQUE (id_subsistema, din_instante)
            );
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Etags (
                id SERIAL PRIMARY KEY,
                URL VARCHAR UNIQUE NOT NULL,
                ETag VARCHAR NOT NULL
            );
        """
        )

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS processed_files (
            file_name VARCHAR PRIMARY KEY,
            last_modified TIMESTAMP
        );
    """
        )
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error("Error creating tables.")
        logging.error(e)


# Function to load ETags from the database
def load_etags_db(cursor):
    try:
        logging.info("Loading ETags from the database.")
        cursor.execute("SELECT URL, ETag FROM Etags")
        etags = cursor.fetchall()
        etags_df = (
            pd.DataFrame(etags, columns=["URL", "ETag"])
            .set_index("URL")
            .to_dict()["ETag"]
        )
        return etags_df  # Return a dictionary URL -> ETag
    except Exception as e:
        logging.error("Failed to load ETags from the database.")
        logging.error(e)
        return {}


# Function to update ETags in the database
def update_etag_db(cursor, url, new_etag):
    try:
        logging.info(f"Updating ETag for {url} in the database.")
        cursor.execute("SELECT * FROM Etags WHERE URL = %s", (url,))
        result = cursor.fetchone()

        if result:
            cursor.execute("UPDATE Etags SET ETag = %s WHERE URL = %s", (new_etag, url))
        else:
            cursor.execute(
                "INSERT INTO Etags (URL, ETag) VALUES (%s, %s)", (url, new_etag)
            )
        logging.info(f"→ Database updated: ETag for {url}")
    except Exception as e:
        logging.error(f"Failed to update ETag for {url} in the database: {e}")
