import logging
import sys
from utils.database import db_connect
from utils.load import process_and_load_files

# Configure logging to show in the notebook
logging.basicConfig(
    level=logging.INFO,  # Changed to DEBUG for more detailed logging
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)

# Step 1: Connect to the database
db_connection = db_connect()
cursor = db_connection.cursor()

# Step 2: Process all files in the 'data/' directory
process_and_load_files(cursor, db_connection, file_dir="data/")

# Step 3: Close the cursor and connection when done
cursor.close()
db_connection.close()
