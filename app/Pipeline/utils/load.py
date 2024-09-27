import os
import logging
import datetime
from .transform import read_csv_file


# Function to delete existing rows for the input_file from the `carga_diaria` table
def delete_existing_rows(cursor, input_file):
    try:
        logging.info(f"Deleting existing rows for {input_file}.")
        cursor.execute("DELETE FROM carga_diaria WHERE input_file = %s;", (input_file,))
    except Exception as e:
        logging.error(f"Failed to delete rows for {input_file}: {e}")
        raise  # Raise the exception to trigger a rollback in the main function


# Function to check if the file has been modified since it was last processed
def is_new_or_modified(cursor, file_name, last_modified):
    try:
        cursor.execute(
            "SELECT last_modified FROM processed_files WHERE file_name = %s;",
            (file_name,),
        )
        result = cursor.fetchone()

        if result is None:
            # The file has never been processed
            logging.info(f"{file_name} has never been processed. It will be loaded.")
            return True

        # Compare the last modified time with the stored one
        db_last_modified = result[0]
        if last_modified > db_last_modified:
            logging.info(f"{file_name} has been modified. It will be reloaded.")
            return True
        else:
            logging.info(f"{file_name} is up-to-date. No need to reload.")
            return False
    except Exception as e:
        logging.error(f"Failed to check modification status for {file_name}: {e}")
        raise


# Function to insert new rows into the database
def insert_file_to_db(cursor, df, input_file):
    try:
        logging.info(f"Inserting new rows for {input_file}.")
        insert_query = """
        INSERT INTO carga_diaria (id_subsistema, nom_subsistema, din_instante, val_cargaenergiamwmed, Ano, input_file)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
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
        cursor.executemany(insert_query, data_to_insert)
    except Exception as e:
        logging.error(f"Failed to insert rows for {input_file}: {e}")
        raise  # Raise the exception to trigger a rollback in the main function


# Main function to process a file and load it into the database
def load_file_to_db(cursor, db_connection, df, input_file):
    try:
        # Step 1: Delete existing rows for this file
        delete_existing_rows(cursor, input_file)

        # Step 2: Insert the new data from the file
        insert_file_to_db(cursor, df, input_file)

        # Step 3: Commit the transaction if everything is successful
        db_connection.commit()
        logging.info(f"Transaction committed successfully for {input_file}.")

    except Exception as e:
        # If any exception occurs, rollback the transaction
        db_connection.rollback()
        logging.error(f"Transaction rolled back due to error for {input_file}: {e}")


# Function to update the processed files table with the new last modified time
def update_processed_file(cursor, db_connection, file_name, last_modified):
    try:
        cursor.execute(
            """
            INSERT INTO processed_files (file_name, last_modified) 
            VALUES (%s, %s)
            ON CONFLICT (file_name) 
            DO UPDATE SET last_modified = EXCLUDED.last_modified;
        """,
            (file_name, last_modified),
        )
        db_connection.commit()
        logging.info(f"Processed files table updated for {file_name}.")
    except Exception as e:
        logging.error(f"Failed to update processed file record for {file_name}: {e}")
        db_connection.rollback()
        raise


# Main function to process and load files only if new or modified
def process_and_load_files(cursor, db_connection, file_dir="data/"):
    logging.info(f"Processing files in directory: {file_dir}")

    failed_files = []  # List to track files that fail during processing

    for file_name in os.listdir(file_dir):
        file_path = os.path.join(file_dir, file_name)

        if file_name.endswith(".csv"):
            last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            # Check if the file is new or has been modified
            if not is_new_or_modified(cursor, file_name, last_modified):
                continue  # Skip this file if it's already up-to-date

            df = read_csv_file(file_path)  # Assuming read_csv_file is defined elsewhere
            if df is not None:
                # Add the year based on the file name (assuming the file name ends with the year)
                try:
                    year = int(file_name.split("_")[-1].replace(".csv", ""))
                except ValueError:
                    logging.error(
                        f"Failed to extract year from {file_name}. Skipping file."
                    )
                    failed_files.append(file_name)
                    continue  # Skip this file if the year is not found

                df["Ano"] = year
                df["input_file"] = file_name

                # Log that the file is being processed
                logging.info(f"Processing and loading file: {file_name}")

                # Load the data into the database (delete old rows, insert new ones)
                try:
                    load_file_to_db(cursor, db_connection, df, file_name)
                    logging.info(
                        f"Successfully loaded data from {file_name} into the database."
                    )

                    # Update the processed files table with the new last modified time
                    update_processed_file(
                        cursor, db_connection, file_name, last_modified
                    )

                except Exception as e:
                    logging.error(f"Error processing {file_name}: {e}")
                    failed_files.append(file_name)
                    continue  # Continue with the next file if an error occurs

    # After processing, report any failed files
    if failed_files:
        logging.warning(f"The following files failed to process: {failed_files}")
