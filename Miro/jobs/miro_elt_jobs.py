#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Script contains function definition to read, process and persist data
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/10/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os
import sys
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

PATH = os.getcwd()

def extract_data(
        spark: SparkSession,
        schema: StructType,
        logger
) -> DataFrame:
    """
    Explanation: Performing Data Load from source directory having files in JSON format
    :param  spark SparkSession: Spark session object
    :param  schema StructType: StructType schema object
    :param  logger: Logger class object
    :return df DataFrame: Spark DataFrame
    """
    logger.info("Started: Data load from input folder")
    try:
        logger.info("Started validating local file input path")
        if os.path.exists(PATH):
            df = spark\
                .read\
                .schema(schema)\
                .json(os.path.join(PATH, "../input/*.json"))
        else:
            log_msg = "ERROR: The {} does not exist".format(PATH)
            logger.error(log_msg)
        logger.info("Successfully validated local file input path")
    except (IOError, OSError) as err:
        logger.error("ERROR: occured validate file path")
        print("ERROR: occured validate file path")
        print(err)
        traceback.print_exception(*sys.exc_info())
        sys.exit(0)
    logger.info("Completed: Data load from input folder")

    # caching dataframe object
    df.cache()
    logger.info("Dataframe cached in memory")

    return df


def transform_data(
        df_list: list,
        logger
) -> None:
    """
    Explanation: Performing right Data Type match for raw Data Load assuming schema is of fixed type
    :param  df DataFrame: Spark DataFrame with raw input
    :param  logger: Logger class object
    :return None: Print output percentage on console
    """
    logger.info("Started: Data Transformation on input Dataframe")
    try:
        df_registered = df_list[0]
        df_app = df_list[1]
        df = df_registered\
                .select(
                    col("initiator_id"),
                    col("event").alias("registered_event"),
                    col("channel").alias("registered_channel"),
                    col("timestamp").alias("registered_timestamp")
                    ) \
            .join(df_app
                .select(
                    col("initiator_id"),
                    col("event").alias("app_event"),
                    col("device_type").alias("app_device_type"),
                    col("timestamp").alias("app_timestamp")
                    ), ["initiator_id"], "leftouter")

        df = df\
            .withColumn("week_of_year_of_register", weekofyear(df.registered_timestamp)) \
            .withColumn("week_of_year_of_app", weekofyear(df.app_timestamp))

        df = df\
            .withColumn("days_passed", datediff(df.app_timestamp, df.registered_timestamp)) \
            .withColumn("weeks_passed", df.week_of_year_of_app - df.week_of_year_of_register)

        df_final = df\
            .filter(
                    (df.days_passed < 14) &
                    (df.days_passed.isNotNull()) &
                    (df.weeks_passed <= 1) &
                    (df.weeks_passed.isNotNull())
                    )
        percent = df_final.distinct().count() / df_registered.distinct().count() * 100
        print(int(percent))
    except Exception as err:
        logger.error("ERROR occured while applying transformation on dataframe")
        print("ERROR occured while applying transformation on dataframe")
        print(err)
        traceback.print_exc()
    logger.info("Completed: Data Transformation on input Dataframe")

def load_data(
        df: DataFrame,
        event_type: str,
        event_columns: str,
        logger
) -> DataFrame:
    """
    Explanation: Write output to directory as CSV
    :param  df DataFrame: DataFrame to print
    :param  event_type String: Type of user event
    :param  event_columns String: Tuple object representing associated dataframe column
    :param  logger Logger: Logger class object
    :return df DataFrame: Spark DataFrame
    """
    initiator_id, event, timestamp, medium = event_columns.split(",")
    df = df\
        .select(initiator_id, event, timestamp, medium)\
        .filter(col("event") == event_type)

    logger.info("Started: Persisting dataset as Parquet file to the output folder")
    try:
        logger.info("Started validating the local file output path")
        if os.path.exists(os.path.join(PATH, "../output")):
            df\
                .coalesce(1)\
                .write\
                .mode('overwrite')\
                .option('header', 'true')\
                .parquet(os.path.join(PATH, "../output/{}".format(event_type)))
        else:
            log_msg = "ERROR: The {} does not exist".format(os.path.join(PATH, "../output"))
            logger.error(log_msg)
        logger.info("Successfully validated the local file output path")
    except (IOError, OSError) as err:
        logger.error("ERROR: occured validate file path")
        print("ERROR: occured validate file path")
        print(err)
        traceback.print_exception(*sys.exc_info())
        sys.exit(0)
    logger.info("Completed: Persisting dataset as CSV to the output folder")

    return df

