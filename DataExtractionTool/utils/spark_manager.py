import config
import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

#################################################
##                                             ##
##          Spark Session Functions            ##
##                                             ##
#################################################

def initialize_spark_session()-> SparkSession:
    '''
    Create an active Spark Session to create an interface to use the Apache Spark ecosystem.

    Returns :
    spark : spark session.
    '''
    spark = SparkSession.builder \
    .master("local") \
    .appName(config.APP_NAME) \
    .enableHiveSupport() \
    .getOrCreate()

    dependency_zip = "dependencies.zip"
    zip_path = os.path.join(os.getcwd(), dependency_zip)

    if os.path.exists(zip_path):
        # Use spark.sparkContext, which is the entry point for the underlying cluster manager
        spark.sparkContext.addPyFile(zip_path)
        logger.info(f"Successfully added '{dependency_zip}' to Spark context for distribution.")
    else:
        logger.warning(
            f"Dependency file '{dependency_zip}' not found at '{zip_path}'. "
            "UDFs may fail if running on a multi-node cluster."
        )

    return spark
