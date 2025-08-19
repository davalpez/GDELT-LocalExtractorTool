import requests
import logging
import zipfile
import os
import config
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date

logger = logging.getLogger(__name__)

#################################################
##                                             ##
##           Data Retrieving Functions         ##
##                                             ##
#################################################

def retrieve_list_available_files(url: str = config.URL) -> requests.Response:
    '''
    """Retrieves a list of available files from a given URL.

    Args:
        url (str): The URL to request data from

    Returns:
        requests.Response (str): File list
    '''

    # We make a request and try to get the information from the URL
    response = requests.get(config.URL)
    if response.status_code != 200:
        print("Failed to retrieve data.")
        exit()

    # The data is separated by tabs, so we separate it and create a PySpark Dataframe

    try:
        response = requests.get(config.URL)
        # This will automatically raise an HTTPError for non-2xx status codes
        response.raise_for_status()
        logger.info(f"Successfully retrieved data from {config.URL}")

        return response

    except requests.exceptions.HTTPError as http_err:
        # Specific handling for HTTP errors 
        logger.error(f"HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        # Re-raising the exception allows the calling code to handle it
        raise
        
    except requests.exceptions.RequestException as req_err:
        # Handling request-related errors r
        logger.critical(f"A critical request error occurred: {req_err}")
        raise

def check_historical_files_size(
    start_date: date | str,
    end_date: date | str,
    dataframe: DataFrame
) -> None:
    """Prints the total file size for a given date range.

    Args:
        start_date: The first date in the range (inclusive).
        end_date: The last date in the range (inclusive).
        dataframe: A DataFrame with "Date" and "FileSizeMB" columns.

    Raises:
        ValueError: If either the start_date or end_date does not exist
            in the DataFrame.
    """
    # Validate that the start and end dates exist in the dataset
    start_exists = dataframe.filter(F.col("Date") == start_date).count()
    end_exists = dataframe.filter(F.col("Date") == end_date).count()

    if not start_exists or not end_exists:
        raise ValueError(
            f"A boundary date was not found. Start exists: {bool(start_exists)}, "
            f"End exists: {bool(end_exists)}"
        )

    # Filter the DataFrame and calculate the total size
    date_filtered_df = dataframe.filter(F.col("Date").between(start_date, end_date))
    total_size_agg = date_filtered_df.agg(F.sum("FileSizeMB")).collect()[0][0]

    # If the range is valid but contains no files, the sum will be None
    total_size = total_size_agg if total_size_agg is not None else 0

    print(f"Total MB in range {start_date} to {end_date}: {total_size:.2f}")

def unzip_all_and_delete(download_dir: str) -> None:
    """Unzips all .zip files in a directory and deletes the archives.
    Args:
        download_dir: The absolute path to the directory containing the .zip files.
    """
    logger.info("Unzipping available files...")
    if not os.path.isdir(download_dir):
        logger.info(f"Error: Directory not found at '{download_dir}'")
        return

    for file_name in os.listdir(download_dir):
        if file_name.endswith(".zip"):
            file_path = os.path.join(download_dir, file_name)
            logger.info(f"Processing {file_path}...")

            try:
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(download_dir)
                    logger.info(f"Successfully unzipped {file_name}.")
            except zipfile.BadZipFile:
                logger.info(f"Error: {file_name} is not a valid or is a corrupted zip file.")
            except Exception as e:
                logger.info(f"An unexpected error occurred while unzipping {file_name}: {e}")
            else:
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted {file_name}.")
                except OSError as e:
                    logger.error(f"Error deleting file {file_name}: {e}")

    logger.info("-----------------")
    logger.info("Files sucessfully unzipped.")
