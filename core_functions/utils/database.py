import psycopg2
import logging
import pandas as pd

# Import the psycopg2 library to connect and interact with PostgreSQL databases.
# Import the logging module to provide logging capabilities.
# Import pandas for data manipulation and analysis.


# Function to connect to the PostgreSQL database
def db_connect():
    try:
        # Establish a connection to the PostgreSQL database using psycopg2.connect()
        conn = psycopg2.connect(
            dbname="current_flow_db",  # Name of the database to connect to
            user="postgres",  # Database user name
            password="admin321",  # Password for the database user
            host="localhost",  # Host where the database server is running
            port="5432",  # Port number on which the database server is listening
        )
        # Log an informational message indicating the connection was successful
        logging.info("Database connection established.")
        return conn  # Return the connection object to be used for database operations
    except Exception as e:
        # If an exception occurs during connection, log an error message
        logging.error("Unable to connect to the database.")
        logging.error(e)  # Log the exception details
        return None  # Return None to indicate that the connection was not established


# Function to create tables if they don't exist
def create_tables(cursor):
    try:
        logging.info("Creating tables if they don't exist.")

        # Execute a SQL command to create the 'carga_diaria' table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS carga_diaria (
                id SERIAL PRIMARY KEY,                -- Auto-incrementing primary key
                id_subsistema VARCHAR NOT NULL,       -- Subsystem ID
                nom_subsistema VARCHAR NOT NULL,      -- Subsystem name
                din_instante VARCHAR NOT NULL,        -- Date and time instant
                val_cargaenergiamwmed VARCHAR,        -- Energy load value in MW average
                Ano INTEGER NOT NULL,                 -- Year
                input_file VARCHAR NOT NULL,          -- Name of the input file
                CONSTRAINT unique_constraint_carga_diaria UNIQUE (id_subsistema, din_instante)
                -- Unique constraint to prevent duplicate entries for the same subsystem and instant
            );
            """
        )

        # Execute a SQL command to create the 'Etags' table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Etags (
                id SERIAL PRIMARY KEY,          -- Auto-incrementing primary key
                URL VARCHAR UNIQUE NOT NULL,    -- URL of the resource (must be unique)
                ETag VARCHAR NOT NULL           -- ETag associated with the URL
            );
            """
        )

        # Execute a SQL command to create the 'processed_files' table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name VARCHAR PRIMARY KEY,  -- Name of the processed file (primary key)
                last_modified TIMESTAMP         -- Timestamp of the last modification
            );
            """
        )

        logging.info("Tables created successfully.")
    except Exception as e:
        # If an exception occurs during table creation, log an error message
        logging.error("Error creating tables.")
        logging.error(e)  # Log the exception details


# Function to load ETags from the database
def load_etags_db(cursor):
    try:
        logging.info("Loading ETags from the database.")

        # Execute a SQL query to select all URLs and ETags from the 'Etags' table
        cursor.execute("SELECT URL, ETag FROM Etags")
        etags = cursor.fetchall()  # Fetch all the results as a list of tuples

        # Convert the results into a pandas DataFrame for easier manipulation
        etags_df = (
            pd.DataFrame(
                etags, columns=["URL", "ETag"]
            )  # Create DataFrame with specified columns
            .set_index("URL")  # Set 'URL' as the index of the DataFrame
            .to_dict()[
                "ETag"
            ]  # Convert the DataFrame to a dictionary mapping URLs to ETags
        )

        # Explanation of pandas functions used:
        # - pd.DataFrame(): Creates a DataFrame object from the data.
        # - set_index(): Sets the specified column as the index of the DataFrame.
        # - to_dict(): Converts the DataFrame into a dictionary format.

        return etags_df  # Return a dictionary mapping URL to ETag
    except Exception as e:
        # If an exception occurs during data loading, log an error message
        logging.error("Failed to load ETags from the database.")
        logging.error(e)  # Log the exception details
        return {}  # Return an empty dictionary in case of failure


# Function to update ETags in the database
def update_etag_db(cursor, url, new_etag):
    try:
        logging.info(f"Updating ETag for {url} in the database.")

        # Check if an entry with the given URL already exists in the 'Etags' table
        cursor.execute("SELECT * FROM Etags WHERE URL = %s", (url,))
        result = cursor.fetchone()  # Fetch one result from the query

        if result:
            # If an entry exists, update the ETag using an SQL UPDATE statement
            cursor.execute("UPDATE Etags SET ETag = %s WHERE URL = %s", (new_etag, url))
        else:
            # If no entry exists, insert a new one using an SQL INSERT statement
            cursor.execute(
                "INSERT INTO Etags (URL, ETag) VALUES (%s, %s)", (url, new_etag)
            )

        logging.info(f"→ Database updated: ETag for {url}")
    except Exception as e:
        # If an exception occurs during the update, log an error message
        logging.error(f"Failed to update ETag for {url} in the database: {e}")
