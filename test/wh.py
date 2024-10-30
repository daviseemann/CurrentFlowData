# Import necessary libraries
import pandas as pd  # For data manipulation and handling DataFrames
import numpy as np  # For numerical operations (though not explicitly used)
import matplotlib.pyplot as plt  # For plotting (not used in this context)
import psycopg2 as pg  # To connect and interact with a PostgreSQL database
import os  # For interacting with the operating system
import sys  # For system-specific parameters and functions
import requests  # To make HTTP requests for fetching data
import logging  # For logging informational messages, warnings, and errors


# Function to connect to the PostgreSQL database
def db_connect():
    try:
        # Establish the connection using the provided credentials
        conn = pg.connect(
            dbname="current_flow_db",  # Name of the database
            user="postgres",  # Database user
            password="admin321",  # Password for the database user
            host="localhost",  # Host where the database is running
            port="5432",  # Port number for the database
        )
    except Exception as e:
        # Print an error message if the connection fails
        print("I am unable to connect to the database")
        print(e)
    return conn  # Return the connection object


# Establish the database connection
db_connection = db_connect()
cursor = db_connection.cursor()  # Create a cursor object to execute database commands


# Class to create necessary tables in the database
class CreateTables:
    def __init__(self, connect_pg):
        self.connect_pg = connect_pg  # Database connection
        self.cursor = connect_pg.cursor()  # Cursor object for executing SQL commands

    # Method to create the 'carga_diaria' table
    def carga_diaria(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS carga_diaria (
                    id SERIAL PRIMARY KEY,
                    id_subsistema VARCHAR NOT NULL,
                    nom_subsistema VARCHAR NOT NULL,
                    din_instante VARCHAR NOT NULL,
                    val_cargaenergiamwmed VARCHAR,
                    Ano INTEGER NOT NULL,
                    input_file VARCHAR NOT NULL,
                    CONSTRAINT unique_constraint_carga_diaria UNIQUE (id_subsistema, din_instante)
            );"""
        )
        self.connect_pg.commit()  # Commit the changes to the database
        print("Table 'carga_diaria' created successfully")

    # Method to create the 'Etags' table
    def Etags(self):
        self.cursor.execute(
            """CREATE TABLE IF NOT EXISTS Etags (
                URL TEXT PRIMARY KEY,
                ETag TEXT
            );"""
        )
        self.connect_pg.commit()  # Commit the changes to the database
        print("Table 'Etags' created successfully")


# Initialize the CreateTables class and create the tables
create_tables = CreateTables(connect_pg=db_connection)
create_tables.carga_diaria()  # Create 'carga_diaria' table
create_tables.Etags()  # Create 'Etags' table

# Configure logging to display messages with timestamp and severity level
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO
    format="%(asctime)s [%(levelname)s] %(message)s",  # Format of the log messages
    handlers=[
        logging.StreamHandler(stream=sys.stdout),  # Output logs to console
        # logging.FileHandler("file_downloads.log"),  # Optionally log to a file
    ],
)


# Function to load ETags from the 'Etags' table in the database
def load_etags_db():
    cursor.execute("SELECT * FROM Etags")  # Fetch all records from 'Etags' table
    etags = cursor.fetchall()  # Retrieve all the fetched records
    etags_df = pd.DataFrame(etags, columns=["URL", "ETag"])  # Create a DataFrame
    logging.info("Loaded ETags from the database.")
    return etags_df  # Return the DataFrame containing ETags


# Function to get the ETag header from a given URL
def get_etag(url):
    try:
        logging.info(f"Fetching ETag for {url}")
        response = requests.head(f"{url}")  # Send a HEAD request to the URL
        response.raise_for_status()  # Raise an exception for bad status codes
        etag = response.headers.get("ETag")  # Get the 'ETag' header from the response
        logging.info(f"→ ETag fetched: {etag}")
        return etag  # Return the ETag value
    except requests.RequestException as e:
        logging.error(f"Failed to fetch ETag for {url}: {e}")
        return None  # Return None if there's an exception


# Function to compare the current ETag with the stored ETag for a URL
def compare_etag(url, new_etag):
    previous_etag = etags_df[etags_df["URL"] == url][
        "ETag"
    ].values  # Retrieve stored ETag
    return (
        len(previous_etag) == 0 or previous_etag[0] != new_etag
    )  # Return True if different


# Function to determine which URLs need to be updated based on ETag comparison
def stage_etag(urls):
    urls_to_update = []  # List to hold URLs that need updating
    logging.info("Checking for updates based on ETag comparison...")
    for url in urls:
        etag = get_etag(url)  # Fetch the current ETag for the URL
        if etag and compare_etag(url, etag):
            urls_to_update.append(url)  # Add to update list if ETag is different
            logging.info(f"✔ URL staged for download: {url}")
        else:
            logging.info(f"✖ URL is up-to-date: {url}")
    return urls_to_update  # Return the list of URLs to update


# Function to download a file from a URL and save it locally
def download_file(url, save_dir="data/"):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)  # Create the directory if it doesn't exist

    try:
        logging.info(f"Starting download: {url}")
        response = requests.get(f"{url}")  # Send a GET request to the URL
        response.raise_for_status()  # Raise an exception for bad status codes
        file_path = os.path.join(
            save_dir, os.path.basename(url)
        )  # Define the file path
        with open(file_path, "wb") as f:
            f.write(response.content)  # Write the content to a file
        logging.info(f"→ Download complete: {url} saved to {file_path}")
    except requests.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")


# Function to update the ETag for a URL in the database
def update_etag_db(url, new_etag):
    try:
        logging.info(f"Updating ETag for {url} in the database.")
        cursor.execute(
            "SELECT * FROM Etags WHERE URL = %s", (url,)
        )  # Check if URL exists
        result = cursor.fetchone()

        if result:
            # Update the existing ETag
            cursor.execute("UPDATE Etags SET ETag = %s WHERE URL = %s", (new_etag, url))
        else:
            # Insert a new ETag record
            cursor.execute(
                "INSERT INTO Etags (URL, ETag) VALUES (%s, %s)", (url, new_etag)
            )

        db_connection.commit()  # Commit the changes
        logging.info(f"→ Database updated: ETag for {url}")

    except Exception as e:
        logging.error(f"Failed to update ETag for {url} in the database: {e}")
        db_connection.rollback()  # Rollback in case of error


# Function to process URLs: compare ETags, download files if needed, and update ETags
def process_urls(urls, save_dir="data/"):
    global etags_df  # Use the global ETags DataFrame

    urls_to_update = stage_etag(urls)  # Get the list of URLs to update

    if urls_to_update:
        logging.info(f"URLs to be updated: {len(urls_to_update)} files")

        for url in urls_to_update:
            logging.info(f"--- Processing {url} ---")
            download_file(url, save_dir)  # Download the file

            # Update the DataFrame with the new ETag after downloading
            new_etag = get_etag(url)
            if new_etag:
                if not etags_df[etags_df["URL"] == url].empty:
                    # Update existing entry
                    etags_df.loc[etags_df["URL"] == url, "ETag"] = new_etag
                else:
                    # Add new entry
                    etags_df.loc[len(etags_df)] = [url, new_etag]

                # Update the ETag in the database
                update_etag_db(url, new_etag)

        logging.info(
            "All URLs have been processed. ETag DataFrame and database updated."
        )
    else:
        logging.info("No files need to be updated. All files are up-to-date.")


# Base URL for the data files
url_base = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/"

years = range(2015, 2025)  # Define the range of years

# Generate the list of URLs for the specified years
urls = [f"{url_base}CARGA_ENERGIA_{year}.csv" for year in years]

# Initialize the ETags DataFrame by loading from the database
etags_df = load_etags_db()

# Process the URLs (download and update ETags as necessary)
process_urls(urls)


# Function to read a CSV file into a DataFrame
def read_csv_file(file_path):
    try:
        logging.info(f"Reading CSV file: {file_path}")
        df = pd.read_csv(file_path, sep=";", decimal=",")  # Read the CSV file
        logging.info(f"File {file_path} read successfully with {len(df)} rows.")
        return df  # Return the DataFrame
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return None


# Function to delete existing rows for a specific file from the 'carga_diaria' table
def delete_existing_rows(input_file):
    try:
        logging.info(f"Deleting existing rows for {input_file} from the database.")
        delete_query = "DELETE FROM carga_diaria WHERE input_file = %s;"  # SQL query
        cursor.execute(delete_query, (input_file,))  # Execute the query
        db_connection.commit()  # Commit the changes
        logging.info(f"→ Rows for {input_file} deleted successfully.")
    except Exception as e:
        logging.error(f"Failed to delete rows for {input_file}: {e}")
        db_connection.rollback()  # Rollback in case of error


# Function to insert data from a DataFrame into the 'carga_diaria' table
def insert_file_to_db(df, input_file):
    try:
        logging.info(f"Inserting new rows for {input_file} into the database.")
        insert_query = """
        INSERT INTO carga_diaria (id_subsistema, nom_subsistema, din_instante, val_cargaenergiamwmed, Ano, input_file)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        # Prepare data for insertion
        data_to_insert = [
            (
                row["id_subsistema"],
                row["nom_subsistema"],
                row["din_instante"],
                row["val_cargaenergiamwmed"],
                row["Ano"],
                input_file,
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(insert_query, data_to_insert)  # Insert multiple records
        db_connection.commit()  # Commit the changes
        logging.info(f"→ Rows for {input_file} inserted successfully.")
    except Exception as e:
        logging.error(f"Failed to insert rows for {input_file}: {e}")
        db_connection.rollback()  # Rollback in case of error


# Function to load data from a file into the database
def load_file_to_db(df, input_file):
    logging.info(f"Loading data from {input_file} into the database.")
    delete_existing_rows(input_file)  # Delete existing data for the file
    insert_file_to_db(df, input_file)  # Insert new data


# Function to process all CSV files in a directory and load them into the database
def process_and_load_files(file_dir="data/"):
    logging.info(f"Processing files in directory: {file_dir}")
    for file_name in os.listdir(file_dir):
        file_path = os.path.join(file_dir, file_name)  # Full path to the file
        if file_name.endswith(".csv"):
            df = read_csv_file(file_path)  # Read the CSV file into a DataFrame
            if df is not None:
                # Extract the year from the file name and add it to the DataFrame
                year = int(file_name.split("_")[-1].replace(".csv", ""))
                df["Ano"] = year
                df["input_file"] = file_name
                # Load the data into the database
                load_file_to_db(df, file_name)


# Process and load all CSV files into the database
process_and_load_files("./data/")


# Function to execute a SQL query and return the result as a DataFrame
def execute_query(query):
    try:
        cur = db_connection.cursor()  # Create a new cursor
        cur.execute(query)  # Execute the query
        result = cur.fetchall()  # Fetch all results
        columns = [desc[0] for desc in cur.description]  # Get column names
        cur.close()  # Close the cursor
        return pd.DataFrame(result, columns=columns)  # Return as DataFrame
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        return None


# Example usage: Display the first 5 rows from 'carga_diaria' table
query = "SELECT * FROM carga_diaria LIMIT 5;"
result = execute_query(query)
display(result)  # Display the result in the notebook
