import os
import logging
import config
import argparse
import requests
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import  StringType
from pyspark.sql import functions as F
from datetime import date

from .utils import (
    setup_logging,
    get_validated_date_input,
    get_confirmation_input,
    valid_date_type,
    initialize_spark_session,
    download_file_task,
    prepare_gdelt_download_df,
    prepare_fileList_dataframe,
    retrieve_list_available_files,
    check_historical_files_size,
    unzip_all_and_delete
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
    # Prepare the DataFrame with all necessary information
    logger.info("Preparing DataFrame with download information...")
    prepared_df = prepare_gdelt_download_df(
        df, start_date, end_date, base_url, output_dir
    )

    #  Wrap our Python function into a Spark UDF
    download_udf = F.udf(download_file_task, StringType())

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

    # Log the final summary
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
    """Parses arguments and runs the data extraction workflow.

    This main entry point handles command-line arguments for start and end dates.
    If arguments are not provided, it falls back to an interactive mode to
    prompt the user for dates. It then proceeds with the main application logic.
    """
    parser = argparse.ArgumentParser(
        description="Data Extractor to calculate historical file sizes."
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
        '-u', '--unzip',
        action='store_const',
        const=1,
        default=0,
        help="Set to 1 to activate the unzip functionality."
    )
    args = parser.parse_args()

    # Determine whether to use arguments or interactive mode
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
        print("Using dates provided from command-line arguments.")
    else:
        print("No command-line dates provided.")
        start_date = get_validated_date_input("Enter the start date (YYYY-MM-DD): ")
        end_date = get_validated_date_input("Enter the end date (YYYY-MM-DD): ")

    # Validate the logical order of dates
    if start_date > end_date:
        print("Error: The start date cannot be after the end date.")
        return

    print(f"Date range selected: {start_date} to {end_date}")

    try:
        spark = initialize_spark_session()
        df = extract_tool(start_date,end_date,spark)
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        download_gdelt_files_distributed(start_date,end_date,df,base_url=config.GDELT_EVENTS_URL,
        output_dir=config.OUTPUT_DIR)
        if args.unzip :
            unzip_all_and_delete(config.OUTPUT_DIR)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()
    
    

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


if __name__ == "__main__":
    main()
    