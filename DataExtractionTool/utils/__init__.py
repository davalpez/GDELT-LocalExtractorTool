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
from .downloader import download_file_task, download_file_task_orq, retrieve_list_available_files
from .spark_transforms import prepare_gdelt_download_df, prepare_fileList_dataframe, read_unzipped_csv_to_df, transform_and_append_parquet,load_headers_from_excel,check_historical_files_size,merge_parquets
from .file_handler import unzip_all_and_delete , clear_directory, delete_files_with_extension, delete_directory_tree

__all__ = [
    'setup_logging',
    'get_validated_date_input',
    'get_confirmation_input',
    'valid_date_type',
    'initialize_spark_session',
    'download_file_task',
    'download_file_task_orq',
    'prepare_gdelt_download_df',
    'prepare_fileList_dataframe',
    'retrieve_list_available_files',
    'check_historical_files_size',
    'unzip_all_and_delete',
    'clear_directory',
    'read_unzipped_csv_to_df',
    'transform_and_append_parquet',
    'delete_files_with_extension',
    'load_headers_from_excel',
    'delete_directory_tree',
    'merge_parquets'
]