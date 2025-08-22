import logging
import zipfile
import os
from pyspark.sql import functions as F
import shutil

logger = logging.getLogger(__name__)

#################################################
##                                             ##
##           Data Cleaning Functions           ##
##                                             ##
#################################################

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


def clear_directory(directory_path: str) -> None:
    """Ensures a directory is empty by deleting and recreating it.

    Args:
        directory_path: The path to the directory to clear.
    """
    logger.info(f"Clearing all contents from directory: {directory_path}")
    try:
        # Check if the path exists and is a directory
        if os.path.isdir(directory_path):
            # shutil.rmtree() is the Python equivalent of 'rm -rf'
            shutil.rmtree(directory_path)
            logger.debug(f"Removed existing directory tree: {directory_path}")
        
        # os.makedirs() will create the directory
        os.makedirs(directory_path, exist_ok=True)
        logger.info(f"Successfully cleared and recreated directory: {directory_path}")
    
    except OSError as e:
        logger.error(f"An OS error occurred while trying to clear directory {directory_path}: {e}")
        raise

def delete_files_with_extension(directory_path: str, extension: str) -> None:
    """Deletes all files with a specific extension from a directory.

    This function is a crucial cleanup step for pipelines, used to remove
    intermediate files (like .csv) after their data has been permanently
    stored in a different format (like .parquet).

    Args:
        directory_path: The path to the directory to clean up.
        extension: The file extension to delete (e.g., '.csv', '.txt').
                   The search is case-insensitive.
    """
    logger.info(f"Searching for '{extension}' files to delete in '{directory_path}'...")
    
    # Safety check: Ensure the path is a valid directory before proceeding.
    if not os.path.isdir(directory_path):
        logger.error(f"Cleanup failed. Directory not found: {directory_path}")
        return

    deleted_count = 0
    # Iterate through every item in the specified directory.
    for filename in os.listdir(directory_path):
        # Use .lower() to make the check case-insensitive (handles .csv, .CSV, etc.).
        if filename.lower().endswith(extension.lower()):
            file_path = os.path.join(directory_path, filename)
            try:
                # Attempt to delete the file.
                os.remove(file_path)
                logger.debug(f"Deleted intermediate file: {file_path}")
                deleted_count += 1
            except OSError as e:
                # Log an error if the file couldn't be deleted (e.g., permissions issue).
                logger.error(f"Error deleting file {file_path}: {e}")
    
    logger.info(
        f"Cleanup complete. Deleted {deleted_count} '{extension}' files "
        f"from the directory."
    )

def delete_directory_tree(directory_path: str) -> None:
    """Recursively deletes a directory and all of its contents.

    Args:
        directory_path: The path to the directory to be deleted.
    """
    logger.info(f"Attempting to delete directory tree: {directory_path}")
    try:
        if os.path.isdir(directory_path):
            shutil.rmtree(directory_path)
            logger.info(f"Successfully deleted directory: {directory_path}")
        else:
            logger.warning(
                f"Skipping deletion. Directory not found: {directory_path}"
            )
    except OSError as e:
        logger.error(
            f"An OS error occurred while trying to delete directory "
            f"{directory_path}: {e}"
        )
        raise