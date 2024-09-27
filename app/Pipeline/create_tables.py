import logging
import sys
from utils.database import db_connect, create_tables

# Configure logging to show in the notebook
logging.basicConfig(
    level=logging.INFO,  # Changed to DEBUG for more detailed logging
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)

# Connect to the database
conn = db_connect()
cursor = conn.cursor()

# Create tables if they don't exist
create_tables(cursor)

# Commit and close the cursor and connection
conn.commit()
cursor.close()
conn.close()
