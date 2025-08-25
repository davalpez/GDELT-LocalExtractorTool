"""
Configuration settings for the GDELT Data Extraction Tool.

This file contains global constants such as URLs, file paths,
and other static configuration values.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.resolve()
ASSETS_DIR = PROJECT_ROOT / "assets"
DATA_DIR = PROJECT_ROOT / "data"
# --- GDELT Project ---

URL = "http://data.gdeltproject.org/events/filesizes"
GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/"

# --- File System ---

OUTPUT_DIR = DATA_DIR / "gdelt_downloaded_data"
UNZIPPED_DIRECTORY = DATA_DIR / "unzipped"
OUTPUT_PARQUET_PATH = DATA_DIR / "processed_parquet/"
MERGED_PARQUET_PATH = DATA_DIR / "merged_parquet/"

# --- Spark Settings ---
APP_NAME = "GDELT_Pipeline"
MASTER_URL = "local[*]"

# --- Assets  ---

HEADERS_EXCEL_PATH = ASSETS_DIR / "gdelt_headers.xlsx"
HEADERS_SHEET_NAME = "CSV.header.dailyupdates"
GDELT_LOOKUP_PATH= ASSETS_DIR / "MASTER-GDELTDOMAINSBYCOUNTRY-MAY2018.txt"
EXTENDED_LOOKUP_PATH= ASSETS_DIR / "extended_lookup.csv"
CAMEO_PATH = ASSETS_DIR / "cameo_dictionary"

#--- Filter Columns to find terms ----

FILTER_TERMS = ["palestine", "west bank", "israel","idf","gaza","PSE","GZS","WSB"] # Example terms to study Israel-Palestine conflict throug news media
FILTER_TERMS_COLUMNS = ['Actor1Name','Actor1Geo_FullName', 'Actor2Name','Actor2Geo_FullName','ActionGeo_FullName','ActionGeo_CountryCode','SOURCEURL']
