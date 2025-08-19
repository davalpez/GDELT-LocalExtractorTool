import os
import requests
import logging
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import date

logger = logging.getLogger(__name__)

#################################################
##                                             ##
##           Data download functions           ##
##                                             ##
#################################################

def DownloadFile(url: str, local_filename: str):

    logger.info(f"Attempting to download: {url}")
    try:
        # Use stream=True for potentially large files
        with requests.get(url, stream=True, timeout=60) as r:
            # Check if the request was successful (status code < 400)
            r.raise_for_status()
            # Check for empty files
            content_length = r.headers.get('content-length')
            if content_length is not None and int(content_length) == 0:
                logger.warning(f"Downloaded file appears empty (0 bytes): {url}. Skipping save.")
                return False # Indicate failure (empty file)
            # Write the file content chunk by chunk
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded and saved to {local_filename}")
            return True 

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"File not found on server (404): {url}")
        else:
            logger.error(f"HTTP Error downloading {url}: {e}")
        # Clean up partially downloaded file if an error occurred
        if os.path.exists(local_filename):
            try:
                os.remove(local_filename)
                logger.info(f"Removed incomplete file: {local_filename}")
            except OSError as oe:
                logger.warning(f"Could not remove partial file {local_filename}: {oe}")
        return False # Indicate failure
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection Error downloading {url}: {e}")
        return False
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout Error downloading {url}: {e}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"General Error downloading {url}: {e}")
        return False
    except Exception as e:
         logger.error(f"An unexpected error occurred for {url}: {e}", exc_info=True)
         return False
    
def download_file_task(url: str, local_filepath: str) -> str:
    """
    Downloads a single file from a URL to a local path.
    This function is designed to be used within a Spark UDF.

    Args:
        url: The URL of the file to download.
        local_filepath: The destination path to save the file.

    Returns:
        A status string
    """
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
        with open(local_filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return "SUCCESS"

    except requests.exceptions.HTTPError as e:
        return f"FAILED: HTTP Error {e.response.status_code}"
    except requests.exceptions.ConnectionError:
        return "FAILED: Connection Error"
    except requests.exceptions.Timeout:
        return "FAILED: Request Timed Out"
    except Exception as e:
        return f"FAILED: An unexpected error occurred - {e}"


def download_file_task_old(file_url: str, local_filepath: str) -> str:
    """
    Downloads a single file and returns its status.
    This is the logic that will be distributed across the Spark cluster.

    Args:
        file_url: URL from where file is downloaded.
        local_filepath: path to download the file.
    """
    try:
        if os.path.exists(local_filepath):
            return "SKIPPED"
        response = requests.get(file_url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes

        with open(local_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return "SUCCESS"
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return "FAILED_NOT_FOUND"
        return "FAILED_HTTP_ERROR"
    except Exception:
        # Clean up partial file on any other error
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
        return "FAILED_UNKNOWN"



