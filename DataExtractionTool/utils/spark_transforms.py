
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from datetime import date
import logging
import os
import json
import config
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from datetime import date
from functools import reduce
from typing import List, Dict
import pandas as pd
from pyspark.sql.column import Column


logger = logging.getLogger(__name__)

#################################################
##                                             ##
##       Dataframe creation  functions         ##
##                                             ##
#################################################

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

    print(f"Total MB in range {start_date} to {end_date}: {total_size:.2f} MB")


def prepare_gdelt_download_df(
    df: DataFrame,
    start_date: date,
    end_date: date,
    base_url: str,
    output_dir: str
) -> DataFrame:
    """
    Filters and transforms the GDELT master list to prepare for downloading.

    Adds columns for the file URL and the target local filepath.

    Args:
        df: The GDELT master file list DataFrame.
        start_date: The start of the date range to filter.
        end_date: The end of the date range to filter.
        base_url: The base URL for GDELT files.
        output_dir: The target directory for downloads.

    Returns:
        A transformed DataFrame ready for the download UDF.
    """
    files_to_download_df = (
        df.filter(F.col("Date").between(start_date, end_date))
        .withColumn("date_str", F.date_format(F.col("Date"), "yyyyMMdd"))
        .withColumn("filename", F.concat(F.col("date_str"), F.lit(".export.CSV.zip")))
        .withColumn("file_url", F.concat(F.lit(base_url), F.col("filename")))
        .withColumn("local_filepath", F.concat(F.lit(output_dir), F.lit("/"), F.col("filename")))
    )
    return files_to_download_df


def prepare_fileList_dataframe(response_text:str,spark:SparkSession)-> DataFrame:
    '''
    Takes the list of data from URL and manages to convert into a usable dataframe to
    specify files with sizes and dates to be downloaded.

    Args:
        response_text (str): Data from response
        spark(SparkSession) : Spark session active

    Returns:
        df (Dataframe): Dataframe with name,size and date.
    '''

    df = Split_Response(response_text,spark)
    df = Transform_Dataframe_Columns(df)
    return df


def Split_Response(response_text:str,spark:SparkSession)-> DataFrame:
    '''
    Manages to split the response 

    Args:
        response_text (str): Data from response
        spark(SparkSession) : Spark session active

    Returns:
        requests.Response (str): File list
    '''
    
    data = [line.split() for line in response_text.strip().split("\n")]
    df = spark.createDataFrame(data, ["FileSize", "FileName"])
    return df


def Transform_Dataframe_Columns(df: DataFrame)-> DataFrame:
    """Transforms columns in the DataFrame, adding Date,Size and FileName.

    Args:
        dataframe (DataFrame): First DataFrame created after receiving the Data List.

    Returns:
        DataFrame: A new DataFrame with all needed transformations.
    """

    # Convert each column using withColumn()
    df = df.withColumn('FileSize', F.col('FileSize').cast(FloatType()))
    # Update dataframe with IDs
    df_id = df.select(F.monotonically_increasing_id().alias("id"), "*")
    # Add a Size Column to the dataframe
    df_mb = df_id.selectExpr("id as id","FileSize * 1e-6 as FileSizeMB","FileName as FileName","FileName as Date")
    df_mb = df_mb.withColumn("FileSizeMB", F.round(df_mb["FileSizeMB"], 2))
    df_mb.createOrReplaceTempView("Filesize_df")
    # Transforms date
    ready_df = transform_date(df_mb)
    return ready_df

def transform_date(dataframe: DataFrame) -> DataFrame:
    """Transforms a string date column into a valid DateType column.

    Args:
        dataframe (DataFrame): The input PySpark DataFrame containing a "Date"

    Returns:
        DataFrame: A new DataFrame with the "Date" column transformed into
            the DateType format (e.g., "2025-08-18")
    """
    # Extract the part of the string before the first period '.'
    transformed_df = dataframe.withColumn(
        "Date", F.regexp_extract(F.col("Date"), r"^([^\.]+)", 1)
    )

    # Filter to keep only rows where the extracted part is exactly 8 digits
    transformed_df = transformed_df.filter(F.col("Date").rlike("^[0-9]{8}$"))

    # Convert the 8-digit string (yyyyMMdd) into a proper DateType
    transformed_df = transformed_df.withColumn(
        "Date", F.to_date(F.col("Date"), "yyyyMMdd")
    )

    return transformed_df



#################################################
##                                             ##
##       Dataframe prepatation functions       ##
##                                             ##
#################################################

def load_headers_from_excel(header_path: str, sheet_name: str) -> List[str]:
    """Loads a list of headers from a specified Excel sheet.

    Note: This function uses pandas and runs on the driver. It's intended for
    loading configuration data at the start of a job.

    Args:
        header_path: The file path to the .xls or .xlsx file.
        sheet_name: The name of the sheet containing the headers.

    Returns:
        A list of strings representing the column headers.
    """
    logger.info(f"Loading headers from '{header_path}' sheet '{sheet_name}'.")
    try:
        xls = pd.ExcelFile(header_path)
        df_headers = pd.read_excel(xls, sheet_name=sheet_name)
        return df_headers.columns.tolist()
    except FileNotFoundError:
        logger.error(f"Header file not found at: {header_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to read headers from Excel file: {e}")
        raise


def read_gdelt_csv(spark: SparkSession, file_path: str, headers: List[str]) -> DataFrame:
    """Reads a GDELT tab-separated CSV file directly into a Spark DataFrame.

    This function uses Spark's native, distributed CSV reader, which is far
    more efficient than reading with pandas on the driver first.

    Args:
        spark: The active SparkSession.
        file_path: The path to the raw .CSV file or a directory of them.
        headers: A list of strings to use as column names.

    Returns:
        A Spark DataFrame with the GDELT data.
    """
    logger.info(f"Reading GDELT CSV data from: {file_path}")
    raw_df = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .option("encoding", "latin-1")
        .load(file_path)
    )
    # GDELT data can sometimes have a different number of columns.
    # Truncate header list if it's longer than the number of columns read.
    num_columns_read = len(raw_df.columns)
    effective_headers = headers[:num_columns_read]
    
    return raw_df.toDF(*effective_headers)


#################################################
##                                             ##
##     Dataframe transformation functions      ##
##                                             ##
#################################################

def select_columns(df: DataFrame, columns_to_keep: List[str]) -> DataFrame:
    """Selects a specific subset of columns from a DataFrame.

    Args:
        df: The input DataFrame.
        columns_to_keep: A list of column names to keep in the final DataFrame.

    Returns:
        A new DataFrame containing only the specified columns.
    """
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    missing_columns = set(columns_to_keep) - set(existing_columns)

    if missing_columns:
        logger.warning(f"Requested columns not found in DataFrame and will be skipped: {missing_columns}")

    if not existing_columns:
        logger.error("None of the requested columns were found in the DataFrame. Returning an empty DataFrame.")
        return df.sparkSession.createDataFrame([], df.schema)

    logger.info(f"Selecting {len(existing_columns)} columns from DataFrame.")
    return df.select(*existing_columns)


def add_cameo_action_columns(df: DataFrame, cameo_map: Column) -> DataFrame:
    """Enriches a DataFrame with CAMEO code descriptions.

    This function creates two new columns, 'Cameo' and 'Cameo_full', by mapping
    the 'EventCode' column against a provided CAMEO map.

    Args:
        df: The input DataFrame, must contain an 'EventCode' column.
        cameo_map: A Spark MapType column created from a CAMEO dictionary,
                   e.g., F.create_map(list_of_items).

    Returns:
        A new DataFrame with added 'Cameo' and 'Cameo_full' columns.
    """
    if "EventCode" not in df.columns:
        logger.error("'EventCode' column not found, cannot add CAMEO descriptions.")
        return df

    # Extract the root CAMEO code (first two digits) for the main category
    main_cameo_code = F.regexp_extract(F.col("EventCode"), r"^\d{2}", 0)

    return df.withColumn("Cameo", cameo_map[main_cameo_code]) \
             .withColumn("Cameo_full", cameo_map[F.col("EventCode")])


def filter_by_terms_in_columns(df: DataFrame, terms: List[str], columns_to_search: List[str]) -> DataFrame:
    """Filters a DataFrame to keep rows containing any of the given terms in any of the specified columns.

    The search is case-insensitive and handles null values gracefully.

    Args:
        df: The input DataFrame.
        terms: A list of search terms (strings).
        columns_to_search: A list of column names in which to search.

    Returns:
        A filtered DataFrame containing only the matching rows.
    """
    if not terms or not columns_to_search:
        logger.warning("No terms or columns provided for filtering. Returning original DataFrame.")
        return df

    # Create a list of conditions, one for each column to search
    all_column_conditions = []
    for col_name in columns_to_search:
        if col_name in df.columns:
            # Coalesce nulls to empty string and convert to lowercase for searching
            searchable_col = F.lower(F.coalesce(F.col(col_name), F.lit("")))
            
            # Create a condition for this column: does it contain term1 OR term2 OR ...
            term_conditions = [searchable_col.contains(term.lower()) for term in terms]
            column_condition = reduce(lambda a, b: a | b, term_conditions)
            all_column_conditions.append(column_condition)

    if not all_column_conditions:
        logger.error(f"None of the specified columns to search were found in the DataFrame: {columns_to_search}")
        return df.sparkSession.createDataFrame([], df.schema)

    # Combine all column conditions: condition_col1 OR condition_col2 OR ...
    final_filter = reduce(lambda a, b: a | b, all_column_conditions)
    
    return df.filter(final_filter)


# --- Aggregation and Utility Functions ---

def aggregate_events_by_url(df: DataFrame) -> DataFrame:
    """
    Aggregates event data by SOURCEURL, providing summary statistics.

    Args:
        df: A DataFrame of GDELT events, containing columns like 'SOURCEURL',
            'Actor1Name', 'AvgTone', etc.

    Returns:
        An aggregated DataFrame with one row per SOURCEURL.
    """
    df_with_structs = df.withColumn("actor_struct", F.struct(F.col("Actor1Name"), F.col("Actor2Name"))) \
                         .withColumn("geo_struct", F.struct(F.col("Actor1Geo_FullName"), F.col("Actor2Geo_FullName")))

    return df_with_structs.groupBy("SOURCEURL").agg(
        F.first("SQLDATE").alias("FirstEventDate"),
        F.count("GLOBALEVENTID").alias("EventCount"),
        F.collect_list("GLOBALEVENTID").alias("EventIDs"),
        F.mean("GoldsteinScale").alias("GoldsteinMean"),
        F.mean("AvgTone").alias("AvgToneMean"),
        F.collect_list("Cameo_full").alias("EventDescriptions"),
        F.collect_list("actor_struct").alias("ActorPairs"),
        F.collect_list("geo_struct").alias("GeoPairs")
    )


#################################################
##                                             ##
##       Dataframe management/util function    ##
##                                             ##
#################################################

def merge_parquets(spark: SparkSession, origin_dir: str, destination_dir: str, coalesce_to: int = 1) -> None:
    """Reads all Parquet files from a directory and writes them into a single new Parquet file.

    Args:
        spark: The active SparkSession.
        origin_dir: The source directory containing potentially many Parquet files.
        destination_dir: The target directory to write the merged Parquet file.
        coalesce_to: The number of output partitions (files). Defaults to 1.
    """
    logger.info(f"Merging Parquet files from '{origin_dir}' into '{destination_dir}'.")
    df = spark.read.option("recursiveFileLookup", "true").parquet(origin_dir)
    
    logger.info(f"Total rows to merge: {df.count()}")
    
    df.coalesce(coalesce_to).write.mode("overwrite").parquet(destination_dir)
    
    logger.info(f"Successfully merged Parquet file(s) written to '{destination_dir}'.")



    ######## In trial ##########

def read_unzipped_csv_to_df(
    spark: SparkSession, directory_path: str, headers: List[str]
) -> DataFrame:
    """
        Reads all CSV files from a directory directly into a Spark DataFrame
        and applies the provided list of headers as column names.

        This function is the direct, scalable replacement for the older pattern of
        reading with Pandas to apply headers.

        Args:
            spark: The active SparkSession.
            directory_path: The path to the directory containing the .CSV files.
            headers: A list of strings to use as the column names for the DataFrame.

        Returns:
            A Spark DataFrame with the combined data from all CSVs and the
            correct column headers applied.
    """
    logger.info(f"Reading all .CSV files from directory: {directory_path}")
    
    # Construct a glob path to read all files ending in .CSV

    try:
        all_files = os.listdir(directory_path)
        csv_files = [
            f for f in all_files if f.lower().endswith('.csv')
        ]
        
        # 2. Create a list of full, absolute paths for Spark.
        #    Using absolute paths is always safer.
        absolute_csv_paths = [
            os.path.join(os.path.abspath(directory_path), f) for f in csv_files
        ]

        if not absolute_csv_paths:
            logger.warning(f"No .CSV files found in directory: {directory_path}")
            # Return an empty DataFrame with the correct schema structure
            return spark.createDataFrame([], schema=spark.createDataFrame([], headers).schema)

    except FileNotFoundError:
        logger.error(f"Directory not found when trying to list CSV files: {directory_path}")
        return spark.createDataFrame([], schema=spark.createDataFrame([], headers).schema)
    
    logger.info(f"Found {len(absolute_csv_paths)} CSV files to load.")
    # -------------------------------
    
    # Step 1: Read the data using Spark's native reader.
    # The columns will be named _c0, _c1, _c2, etc. at this stage.
    raw_df_with_default_names = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "false")
        .load(absolute_csv_paths)
    )

    # Step 2: Apply the provided headers.
    # The toDF() method is a highly efficient way to rename all columns at once.
    num_columns_read = len(raw_df_with_default_names.columns)
    
    # Handle cases where the number of headers might not match the data.
    if num_columns_read != len(headers):
        logger.warning(
            f"Mismatch between number of columns in data ({num_columns_read}) "
            f"and number of headers provided ({len(headers)}). "
            "Truncating header list to match data."
        )
        effective_headers = headers[:num_columns_read]
    else:
        effective_headers = headers

    # Rename the columns from _c0, _c1, ... to your meaningful names.
    df_with_headers = raw_df_with_default_names.toDF(*effective_headers)

    logger.info(
        f"Successfully loaded {df_with_headers.count()} records and applied "
        f"{len(df_with_headers.columns)} headers."
    )
    return df_with_headers
    
def transform_and_append_parquet(
    input_df: DataFrame,
    output_path: str,
    columns_to_keep: List[str],
    cameo_map: Column,
    filter_bool:bool,
    filter_terms: List[str],
    filter_columns: List[str]
) -> None:
        """
        Applies a full transformation pipeline to a raw DataFrame and appends it to Parquet.


        Args:
            raw_df: The input DataFrame, fresh from the CSV reader.
            output_path: The destination path for the final Parquet file.
            columns_to_keep: A list of final columns to select.
            cameo_map: A Spark MapType column for CAMEO enrichment.
            filter_terms: A list of terms to filter the data by.
            filter_columns: A list of columns to search for the filter terms.
        """
        logger.info(f"Starting full transformation pipeline...")

        # --- Transformation Step 1: Final Column Selection ---
        
        filter_df = select_columns(input_df, columns_to_keep)

        # --- Transformation Step 2: Column reduction Filtering ---
        if filter_bool:
            filter_df = filter_by_terms_in_columns(
            filter_df,
            terms=filter_terms,
            columns_to_search=filter_columns
        )

        # --- Transformation Step 3: Enrichment ---
        
        load_and_create_cameo_map(cameo_map)
        final_df = add_cameo_action_columns(filter_df, cameo_map)

        # --- I/O Step: Writing ---
        logger.info(f"Writing transformed data to Parquet at: {output_path}")
        (
            final_df.write
            .mode("append")
            .parquet(output_path)
        )
        logger.info("Successfully appended chunk to Parquet file.")


def load_and_create_cameo_map(cameo_json_path: str) -> Column:
    """Loads a CAMEO dictionary from a JSON file and creates a Spark MapType column.

    Args:
        cameo_json_path: The file path to the CAMEO JSON dictionary.

    Returns:
        A Spark Column of MapType(StringType(), StringType())
    Raises:
        FileNotFoundError: If the cameo_json_path does not exist.
        json.JSONDecodeError: If the file is not a valid JSON.
    """
    logger.info(f"Loading CAMEO dictionary from: {cameo_json_path}")
    try:
        with open(cameo_json_path, 'r', encoding='utf-8') as f:
            cameo_dict: Dict[str, str] = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load or parse CAMEO JSON file: {e}")
        raise

    map_items = [item for pair in cameo_dict.items() for item in pair]
    
    cameo_map_col = F.create_map(*[F.lit(item) for item in map_items])
    
    logger.info("Successfully created Spark CAMEO map column.")
    return cameo_map_col

def merge_parquets(
    spark: SparkSession,
    origin_dir: str,
    destination_dir: str,
    num_output_files: int = 1
) -> None:
    """Reads a partitioned Parquet dataset and writes it to a new location
    with a specified number of files.

    Args:
        spark: The active SparkSession.
        origin_dir: The source directory containing the partitioned Parquet dataset.
        destination_dir: The target directory to write the merged Parquet file(s).
        num_output_files: The desired number of output files. Use 1 for a single
                          file, or a larger number for bigger datasets.
    """
    logger.info(f"Merging Parquet files from '{origin_dir}' into '{destination_dir}'.")
    try:
        df = spark.read.parquet(origin_dir)
        
        logger.info(f"Total rows to merge: {df.count()}")
        
        # .coalesce() is efficient for reducing partitions.
        df.coalesce(num_output_files).write.mode("overwrite").parquet(destination_dir)
        
        logger.info(
            f"Successfully merged data into {num_output_files} file(s) "
            f"at '{destination_dir}'."
        )
    except Exception as e:
        logger.error(f"Failed to merge Parquet files: {e}")
        raise