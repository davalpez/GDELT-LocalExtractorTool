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
    logger.info(f"Initializing SparkSession.")
    
    spark = (
        SparkSession.builder
        .appName(config.APP_NAME)
        .master(config.MASTER_URL)
        .enableHiveSupport()  # Optional: can be removed if not interacting with Hive tables.
        .getOrCreate()
    )
    
    logger.info("SparkSession initialized successfully.")
    return spark
