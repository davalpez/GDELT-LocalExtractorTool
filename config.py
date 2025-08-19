"""
Configuration settings for the GDELT Data Extraction Tool.

This file contains global constants such as URLs, file paths,
and other static configuration values.
"""
# --- GDELT Project ---

URL = "http://data.gdeltproject.org/events/filesizes"
GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/"

# --- File System ---
OUTPUT_DIR = "data/gdelt_downloaded_data"
UNZIPPED_DIRECTORY = "data/unzipped"
OUTPUT_PARQUET_PATH = "data/processed_parquet/"

# --- Spark Settings ---
APP_NAME = "GDELT_Pipeline"
MASTER_URL = "local[*]"