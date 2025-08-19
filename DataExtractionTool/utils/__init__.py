__version__ = "1.0.0"
__author__ = "David Valdivieso"

"""
Utility Package for the Data Extraction Tool

This package provides a centralized API for helper functions related to:
- Logging setup
- Input validation
- Spark session management
- File downloading and handling
- Spark DataFrame transformations
"""

from .logger_config import setup_logging
from .input_validation import get_validated_date_input, get_confirmation_input, valid_date_type
from .spark_manager import initialize_spark_session
from .downloader import download_file_task
from .spark_transforms import prepare_gdelt_download_df, prepare_fileList_dataframe
from .file_handler import retrieve_list_available_files, check_historical_files_size, unzip_all_and_delete

__all__ = [
    'setup_logging',
    'get_validated_date_input',
    'get_confirmation_input',
    'valid_date_type',
    'initialize_spark_session',
    'download_file_task',
    'prepare_gdelt_download_df',
    'prepare_fileList_dataframe',
    'retrieve_list_available_files',
    'check_historical_files_size',
    'unzip_all_and_delete',
]