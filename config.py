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
MERGED_PARQUET_PATH = "data/merged_parquet/"

# --- Spark Settings ---
APP_NAME = "GDELT_Pipeline"
MASTER_URL = "local[*]"

# --- Assets  ---

HEADERS_EXCEL_PATH = "assets/gdelt_headers.xlsx"
HEADERS_SHEET_NAME = "CSV.header.dailyupdates"
CAMEO_PATH = "assets/cameo_dictionary"

#--- Filter Columns ----

FILTER_TERMS = ["palestine", "west bank", "israel","idf","gaza","PSE","GZS","WSB"]
FILTER_COLUMNS = ['Actor1Name','Actor1Geo_FullName', 'Actor2Name','Actor2Geo_FullName','ActionGeo_FullName','ActionGeo_CountryCode','SOURCEURL']
