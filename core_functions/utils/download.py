import os
import requests
import logging
from utils.database import update_etag_db


# Function to get the ETag from a URL
def get_etag(url):
    try:
        logging.info(f"Fetching ETag for {url}")
        response = requests.head(f"{url}")
        response.raise_for_status()  # Ensure the request was successful
        etag = response.headers.get("ETag")
        logging.info(f"→ ETag fetched: {etag}")
        return etag
    except requests.RequestException as e:
        logging.error(f"Failed to fetch ETag for {url}: {e}")
        return None


# Function to stage URLs for download if their ETag has changed
def stage_etag(urls, etags_df):
    urls_to_update = []
    logging.info("Checking for updates based on ETag comparison...")
    for url in urls:
        etag = get_etag(url)
        if etag and compare_etag(url, etag, etags_df):
            urls_to_update.append(url)
            logging.info(f"✔ URL staged for download: {url}")
        else:
            logging.info(f"✖ URL is up-to-date: {url}")
    return urls_to_update


# Function to compare the current ETag with the stored ETag
def compare_etag(url, new_etag, etags_df):
    previous_etag = etags_df.get(url)
    return previous_etag is None or previous_etag != new_etag


# Function to download the file from the URL and save it
def download_file(url, save_dir="data/"):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    try:
        logging.info(f"Starting download: {url}")
        response = requests.get(f"{url}")
        response.raise_for_status()  # Ensure the request was successful
        file_path = os.path.join(save_dir, os.path.basename(url))
        with open(file_path, "wb") as f:
            f.write(response.content)
        logging.info(f"→ Download complete: {url} saved to {file_path}")
    except requests.RequestException as e:
        logging.error(f"Error downloading {url}: {e}")


# Function to process URLs: compare ETags, download if necessary
def process_urls(urls, save_dir="data/", etags_df=None, cursor=None):
    urls_to_update = stage_etag(urls, etags_df)

    if urls_to_update:
        logging.info(f"URLs to be updated: {len(urls_to_update)} files")

        # Step 2: Download the files for the staged URLs
        for url in urls_to_update:
            logging.info(f"--- Processing {url} ---")
            download_file(url, save_dir)

            # Update the DataFrame with the new ETag after downloading
            new_etag = get_etag(url)
            if new_etag:
                etags_df[url] = new_etag
                # Step 3: Update the ETag in the database
                update_etag_db(cursor, url, new_etag)

        logging.info(
            "All URLs have been processed. ETag DataFrame and database updated."
        )
    else:
        logging.info("No files need to be updated. All files are up-to-date.")
