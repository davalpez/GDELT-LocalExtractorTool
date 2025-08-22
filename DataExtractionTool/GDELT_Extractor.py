import os
import logging
import argparse
from datetime import date, timedelta


import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import config
from DataExtractionTool.utils import schema
from DataExtractionTool.utils import (
    setup_logging,
    get_validated_date_input,
    get_confirmation_input,
    valid_date_type,
    initialize_spark_session,
    download_file_task_orq,
    prepare_gdelt_download_df,
    prepare_fileList_dataframe,
    retrieve_list_available_files,
    check_historical_files_size,
    unzip_all_and_delete,
    clear_directory,
    read_unzipped_csv_to_df,
    transform_and_append_parquet,
    delete_files_with_extension,
    load_headers_from_excel,
    delete_directory_tree,
    merge_parquets
)

#### Logging Options ####

setup_logging()
logger = logging.getLogger(__name__)

#### Function Definition ####


#################################################
##                                             ##
##           Data download functions           ##
##                                             ##
#################################################


def download_gdelt_files_distributed(
    start_date: date,
    end_date: date,
    df: DataFrame,
    base_url: str,
    output_dir: str,
) -> None:
    """Downloads GDELT files in a distributed manner using a UDF.

    Args:
        start_date (date): The first date in the range (inclusive).
        end_date (date): The last date in the range (inclusive).
        dataframe (DataFrame): The master file list DataFrame.
        base_url (str): The base URL for the GDELT files.
        output_dir (str): The path to a SHARED directory where files will be
            saved, accessible from all Spark workers.
    """
    
    logger.info("Preparing DataFrame with download information...")
    prepared_df = prepare_gdelt_download_df(
        df, start_date, end_date, base_url, output_dir
    )

    #  Wrap our Python function into a Spark UDF
    download_udf = F.udf(download_file_task_orq, StringType())

    # Apply the UDF to create a 'download_status' using  a LAZY transformation. No downloads have started yet.
    logger.info("Applying UDF to DataFrame. This will trigger downloads when an action is called.")
    result_df = prepared_df.withColumn(
        "download_status", download_udf(F.col("file_url"), F.col("local_filepath"))
    )

    # Trigger the downloads with an action and get the summary
    # The groupBy() and count() action will execute the UDF for each row in parallel
    # across the cluster.

    logger.info("Triggering download action and collecting summary...")
    summary_df = result_df.groupBy("download_status").count()
    summary_rows = summary_df.collect()
    logger.info("--- Distributed Download Summary ---")
    if not summary_rows:
        logger.warning("No files were processed.")
    else:
        for row in summary_rows:
            logger.info(f"{row['download_status']}: {row['count']}")
    logger.info("----------------------------------")



#################################################
##                                             ##
##                 Main Functions              ##
##                                             ##
#################################################


def main() -> None:
    """Main entry point to parse arguments and run the GDELT data pipeline."""

    parser = argparse.ArgumentParser(
        description="A pipeline to download, process, and store GDELT event data."
    )

    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5,
        help="The number of days to process in each chunk. Default is 5."
    )
    parser.add_argument(
        "-s", "--start_date",
        type=valid_date_type,
        help="The start date for the range in YYYY-MM-DD format."
    )
    parser.add_argument(
        "-e", "--end_date",
        type=valid_date_type,
        help="The end date for the range in YYYY-MM-DD format."
    )
    parser.add_argument(
        '-f', '--filter',
        action='store_const',
        const=1,
        default=0,
        help="Set to 1 to retrieve filtering from config file"
    )
    parser.add_argument(
        '-u', '--only_downloand_and_unzip',
        action='store_const',
        const=1,
        default=0,
        help="Set 1 to only download files and unzip them as csv, no filtering done."
    )

    args = parser.parse_args()

    if args.start_date and args.end_date:
        start_date, end_date = args.start_date, args.end_date
        logger.info(
            "Using dates from command-line arguments: %s to %s", start_date, end_date
        )
    else:
        logger.info("No command-line dates provided. Entering interactive mode.")
        start_date = get_validated_date_input("Enter the start date (YYYY-MM-DD): ")
        end_date = get_validated_date_input("Enter the end date (YYYY-MM-DD): ")

    if start_date > end_date:
        logger.error("The start date cannot be after the end date. Exiting.")
        return

    spark = None
    try:

        logger.info("Loading GDELT headers from Excel file...")
        gdelt_headers = load_headers_from_excel(
            header_path=str(config.HEADERS_EXCEL_PATH),
            sheet_name=config.HEADERS_SHEET_NAME
        )

        logger.info("Initializing Spark Session...")
        spark = initialize_spark_session()

        logger.info("Retrieving the master list of all available files...")
        master_df = prepare_fileList_dataframe(
            retrieve_list_available_files(config.URL).text, spark
        )

        confirmed = False
        while not confirmed:
            check_historical_files_size(start_date, end_date, master_df)
            confirmed = get_confirmation_input("Do you want to proceed? (yes/no): ")
            if not confirmed:
                logger.info("User opted to select a new date range.")
                start_date = get_validated_date_input("Enter new start date: ")
                end_date = get_validated_date_input("Enter new end date: ")

            """         if args.only_downloand_and_unzip:

            logger.info("Running application in only donwloand and unzip mode.")
            try:
                df = extract_tool(start_date,end_date,spark)
                os.makedirs(config.OUTPUT_DIR, exist_ok=True)
                download_gdelt_files_distributed(start_date,end_date,df,base_url=config.GDELT_EVENTS_URL,
                output_dir=config.OUTPUT_DIR)
                unzip_all_and_delete(config.OUTPUT_DIR)

            except Exception as e:
                print(f"An error occurred: {e}") """


            run_pipeline_in_chunks(
                spark=spark,
                start_date=start_date,
                end_date=end_date,
                master_df=master_df,
                gdelt_headers=gdelt_headers,
                chunk_size_days=args.chunk_size,
                filter=args.filter,
                only_download = args.only_downloand_and_unzip
            )

            if not args.only_downloand_and_unzip :
                run_post_processing(
                spark,
                individual_parquet_folder=str(config.OUTPUT_PARQUET_PATH),
                merged_parquet_folder= str(config.MERGED_PARQUET_PATH)
                )

    except Exception:
        logger.exception(
            "An unhandled error occurred in the main workflow. "
            "The application will now exit."
        )
    finally:
        # --- C) TEARDOWN ---
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()


def run_post_processing(spark: SparkSession,individual_parquet_folder: str,merged_parquet_folder:str) -> None:
    """
    Runs final post-processing tasks, including merging chunked Parquet files
    and cleaning up the intermediate data.
    """
    logger.info("=" * 60)
    logger.info("Starting post-processing phase.")
    
    try:

        merge_parquets(
            spark=spark,
            origin_dir=individual_parquet_folder,
            destination_dir=merged_parquet_folder,
            num_output_files=1
        )

        logger.info(
            "Merge successful. Cleaning up intermediate chunked Parquet directory..."
        )
        delete_directory_tree(individual_parquet_folder)

        logger.info("Post-processing and cleanup complete.")
        logger.info(
            "Final, merged data is available at: %s",
            str(config.MERGED_PARQUET_PATH)
        )

    except Exception as e:
        logger.error(
            "Post-processing failed. The intermediate chunked data has been "
            "kept for debugging. Please check the error below and the contents "
            f"of '{str(config.OUTPUT_PARQUET_PATH)}'."
        )

        raise e


def extract_tool(start_date:str,end_date:str,spark:SparkSession):
    
    try:
        file_list_response = retrieve_list_available_files(config.URL)  
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve the file list. The application cannot continue.")
        spark.stop()
        exit()
    try :

        df = prepare_fileList_dataframe(file_list_response.text,spark)
        check_historical_files_size(start_date,end_date,df)
        confirmed = False
        while not confirmed:
            confirmed = get_confirmation_input("Do you want to proceed with downloading files? (yes/no): ")
            if not confirmed:
                print(" Select new date range : ")
                start_date = get_validated_date_input("Enter the start date (YYYY-MM-DD): ")
                end_date = get_validated_date_input("Enter the end date (YYYY-MM-DD): ")
                if start_date > end_date:
                    print("Error: The start date cannot be after the end date.")
                    return
                check_historical_files_size(start_date,end_date,df)
                    
    except Exception as e:
        print(f"An error occurred: {e}")
        if df is not None:
            print("DataFrame has some content, but an error occurred.")

    return df

def run_pipeline_in_chunks(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    master_df: DataFrame,
    gdelt_headers: list,
    filter: bool,
    only_download: bool,
    chunk_size_days: int = 10
    ) -> None:
    """
    Orchestrates the GDELT ETL pipeline, processing the date range in chunks.

    For each chunk, it downloads, unzips, transforms, and appends data to a
    final Parquet destination, cleaning up intermediate files afterward.
    """
    logger.info("Starting GDELT pipeline in chunked mode.")
    
    current_start = start_date
    while current_start <= end_date:
        chunk_end = current_start + timedelta(days=chunk_size_days - 1)
        if chunk_end > end_date:
            chunk_end = end_date

        logger.info("=" * 60)
        logger.info(f"Processing chunk: {current_start} to {chunk_end}")
        logger.info("=" * 60)

        # Ensure the download directory is empty before starting a new chunk.
        clear_directory(str(config.OUTPUT_DIR))

        # STEP 1: DISTRIBUTED DOWNLOAD (using the function above)
        download_gdelt_files_distributed(
            start_date=current_start,
            end_date=chunk_end,
            df=master_df,
            base_url=str(config.GDELT_EVENTS_URL),
            output_dir=str(config.OUTPUT_DIR),
        )

        # STEP 2: UNZIP & DELETE ZIPS
        unzip_all_and_delete(str(config.OUTPUT_DIR))

        if not only_download:
            # STEP 3: READ CSVS
            raw_chunk_df = read_unzipped_csv_to_df(spark, str(config.OUTPUT_DIR), gdelt_headers )
            
            # STEP 4: TRANSFORM & APPEND TO PARQUET
            if not raw_chunk_df.head(1):
                logger.warning(
                    f"No data found for chunk {current_start} to {chunk_end}. "
                    "Skipping transform and append."
                )
            else:
                transform_and_append_parquet(spark=spark,
                input_df=raw_chunk_df,
                output_path=str(config.OUTPUT_PARQUET_PATH),
                columns_to_keep= schema.COLUMNS_TO_KEEP_SHORT,
                cameo_map=str(config.CAMEO_PATH),
                filter_bool=filter,
                filter_terms=config.FILTER_TERMS,
                filter_columns=config.FILTER_TERMS_COLUMNS,
                gdelt_domain_lookup_path=str(config.GDELT_LOOKUP_PATH),
                manual_domain_lookup_path=str(config.EXTENDED_LOOKUP_PATH))
                logger.info(
                    f"Successfully processed and saved chunk: {current_start} "
                    f"to {chunk_end}"
                )


            delete_files_with_extension(str(config.OUTPUT_DIR), extension=".CSV") 
            logger.info(
                "All chunks processed successfully. Final data is available at %s",
                str(config.OUTPUT_PARQUET_PATH),
            )
        current_start = chunk_end + timedelta(days=1)


if __name__ == "__main__":
    main()
    