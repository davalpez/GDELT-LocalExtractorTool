import pytest
import os
import shutil
import config
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pathlib import Path

from DataExtractionTool.utils.spark_transforms import (
    add_domain_and_country_news_source,
    transform_and_append_parquet,
    load_and_create_cameo_map,
    read_unzipped_csv_to_df,
    load_headers_from_excel,
    filter_by_terms_in_columns,
    select_columns,
    add_cameo_action_columns
)
from DataExtractionTool.utils.schema import COLUMNS_TO_KEEP_SHORT

#################################################
##                                             ##
##              Fixture Chain                  ##
##                                             ##
#################################################

current_file_path = Path(__file__)
test_dir = current_file_path.parent
base_dir = test_dir.parent
upper_dir = base_dir.parent
test_data_path = test_dir / "test_data"
path_to_headers = upper_dir / config.HEADERS_EXCEL_PATH

LARGE_COLUMN_LENGHT = 58
SHORT_COLUMN_LENGTH = 19

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """A session-wide SparkSession fixture."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("GDELLocalExtractorTests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()

# STAGE 1: Raw DataFrame
@pytest.fixture(scope="module")
def raw_df(spark):
    gdelt_headers = load_headers_from_excel(header_path=path_to_headers, sheet_name=config.HEADERS_SHEET_NAME)
    df = read_unzipped_csv_to_df(spark, str(test_data_path), gdelt_headers)
    return df

@pytest.fixture(scope="module") 
def filtered_df(raw_df):
    df = select_columns(raw_df, COLUMNS_TO_KEEP_SHORT)
    return df

@pytest.fixture(scope="module") 
def filtered_term_df(filtered_df):
    filter_terms = ["AUSTRALIAN"]
    df = filter_by_terms_in_columns(
        filtered_df,
        terms=filter_terms,
        columns_to_search=config.FILTER_TERMS_COLUMNS
    )
    return df
@pytest.fixture(scope="module") 
def domain_added_df(spark,filtered_df):
    df = add_domain_and_country_news_source(
        spark=spark,
        df=filtered_df,
        gdelt_domain_lookup_path=str(config.GDELT_LOOKUP_PATH),
        manual_domain_lookup_path=str(config.EXTENDED_LOOKUP_PATH)
    )
    return df

@pytest.fixture(scope="module")
def cameo_path():
    return str(config.CAMEO_PATH)

@pytest.fixture(scope="module") 
def cameo_df(cameo_path,domain_added_df):
    cameo_map_col = load_and_create_cameo_map(str(cameo_path))
    df = add_cameo_action_columns(domain_added_df, cameo_map_col)

    return df

@pytest.fixture
def clean_test_output_dir():
    test_data_output_path = test_dir / "test_data" / "test_data_output"
    if test_data_output_path.exists():
        shutil.rmtree(test_data_output_path)
    os.makedirs(test_data_output_path,exist_ok=True)
    yield test_data_output_path
    
    # --- CLEANUP (Teardown) ---
    print(f"\n--- TEARDOWN: Deleting directory and its contents: {test_data_output_path} ---")
    shutil.rmtree(test_data_output_path)

#################################################
##                                             ##
##               Test Suite                    ##
##                                             ##
#################################################

def test_raw_df_creation(raw_df):
    """Tests the output of the raw_df fixture."""
    sql_date = '20230201'
    first_row = raw_df.first()

    assert isinstance(raw_df, DataFrame)
    assert len(raw_df.columns) == LARGE_COLUMN_LENGHT
    assert first_row['SQLDATE'] == sql_date

def test_select_columns(filtered_df):
    """Tests the result of the select_columns function via the filtered_df fixture."""
    assert len(filtered_df.columns) == SHORT_COLUMN_LENGTH

def test_filter_by_terms_in_columns(filtered_term_df):
    """Tests the ideal case: filter_by_terms_in_columns."""
    filter_actor = "AUSTRALIAN"

    
    assert isinstance(filtered_term_df, DataFrame)
    assert filtered_term_df.first()['Actor2Name'] == filter_actor

def test_add_domain_and_country_news_source(domain_added_df):
    """Tests the ideal case: add_domain_and_country_news_source."""
    source_country_added = 'Australia'
    assert isinstance(domain_added_df, DataFrame)
    assert domain_added_df.first()['news_source_country'] == source_country_added

def test_add_cameo_action_columns(cameo_df): 
    """Tests the ideal case: add_domain_and_country_news_source."""
    first_cameo = 'ENGAGE IN DIPLOMATIC COOPERATION'
    assert isinstance(cameo_df, DataFrame)
    assert 'Cameo' in cameo_df.columns
    assert cameo_df.first()['Cameo'] == first_cameo

def test_transform_and_append_parquet_pipeline_with_filter(spark,raw_df,clean_test_output_dir):
    """Tests the ideal case: pipeline for transform_and_append_parquet_pipeline"""

    test_data_output_path = clean_test_output_dir
    filter = True
    filter_terms = ["AUSTRALIAN"]

    os.path.isdir(test_data_output_path)

    transform_and_append_parquet(spark=spark,
            input_df=raw_df,
            output_path=str(test_data_output_path),
            columns_to_keep= COLUMNS_TO_KEEP_SHORT,
            cameo_map=str(config.CAMEO_PATH),
            filter_bool=filter,
            filter_terms=filter_terms,
            filter_columns=config.FILTER_TERMS_COLUMNS,
            gdelt_domain_lookup_path=str(config.GDELT_LOOKUP_PATH),
            manual_domain_lookup_path=str(config.EXTENDED_LOOKUP_PATH))

    assert (test_data_output_path / "_SUCCESS").is_file()

def test_transform_and_append_parquet_pipeline_without_filter(spark,raw_df,clean_test_output_dir):
    """Tests the ideal case: pipeline for transform_and_append_parquet_pipeline"""

    test_data_output_path = clean_test_output_dir
    filter = False
    filter_terms = ["AUSTRALIAN"]

    os.path.isdir(test_data_output_path)

    transform_and_append_parquet(spark=spark,
            input_df=raw_df,
            output_path=str(test_data_output_path),
            columns_to_keep= COLUMNS_TO_KEEP_SHORT,
            cameo_map=str(config.CAMEO_PATH),
            filter_bool=filter,
            filter_terms=filter_terms,
            filter_columns=config.FILTER_TERMS_COLUMNS,
            gdelt_domain_lookup_path=str(config.GDELT_LOOKUP_PATH),
            manual_domain_lookup_path=str(config.EXTENDED_LOOKUP_PATH))

    assert (test_data_output_path / "_SUCCESS").is_file()