import pandas as pd
from utils.download import process_urls
from utils.database import load_etags_db, db_connect
import logging
import sys

# Configure logging to show in the notebook
logging.basicConfig(
    level=logging.INFO,  # Changed to DEBUG for more detailed logging
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)


# Connect to the database
cursor = db_connect().cursor()

# Define the base URL for the API
url_base = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/carga_energia_di/"

# Define the range of years for which we want to fetch data
years = range(2015, 2025)

# Generate the list of URLs for each year
urls = [f"{url_base}CARGA_ENERGIA_{year}.csv" for year in years]

# Simulate loading etags from an existing database or file
etags_df = load_etags_db(cursor)

# Process and download the files if ETags are new/changed
process_urls(urls, save_dir="../data/", etags_df=etags_df)
