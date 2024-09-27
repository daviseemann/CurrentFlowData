import os
import pandas as pd
import logging


# Function to read and transform the CSV file
def read_csv_file(file_path):
    try:
        logging.info(f"Reading CSV file: {file_path}")
        df = pd.read_csv(file_path, sep=";", decimal=",")
        return df
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None


# Function to add the year based on the filename
def add_year_to_df(df, file_name):
    year = int(file_name.split("_")[-1].replace(".csv", ""))
    df["Ano"] = year
    return df
