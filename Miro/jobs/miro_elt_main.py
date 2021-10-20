#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Driver Script to read, process and persist data
#
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/10/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os
import configparser

from Miro_Task.Miro.jobs.miro_elt_jobs import extract_data, transform_data, load_data
from Miro_Task.Miro.project_utils.utils import MiroUtils
from Miro_Task.Miro.project_utils.log4j_root_logger import Log4jRootLogger
from pyspark.sql import SparkSession

PATH = os.getcwd()

def main():
    """
        Main ETL script definition
        :return None :
    """

    # Creating SparkContext
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Miro_Temp | Create Data Loader") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "8") \
        .config("spark.dynamicAllocation.initialExecutors", "4") \
        .config("spark.executor.heartbeatInterval", "200000") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.driver.maxResultSize", "8g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memory", "10g") \
        .config("spark.executor.cores", "3") \
        .config("spark.cores.max", "4") \
        .getOrCreate()

    # Creating log4j logger for logging
    logger = Log4jRootLogger(spark)

    logger.info("################################################################")
    logger.info("########## Starting Execution of Miro_Temp Data Load Job ############")
    logger.info("################################################################")

    logger.info("Started: reading user configuration from config file")

    # Reading configs
    config = configparser.ConfigParser()
    config.read(r"{}/../config/config.ini".format(PATH))

    # Capturing configs for first source
    schema_config = config.get('schema', 'landingFileSchema')
    schema = MiroUtils.read_schema(schema_config)
    event_specification_user = config.get('event_specification', 'user_registeration')
    event_specification_app = config.get('event_specification', 'application_loading')

    logger.info("Completed: reading user configuration from config file")

    user_event_1 = {"source_event_type": "registered", "schema": schema, "event_specification": event_specification_user}
    user_event_2 = {"source_event_type": "app_loaded", "schema": schema, "event_specification": event_specification_app}
    dict_param = {
                    "source_1": user_event_1,
                    "source_2": user_event_2,
                  }
    df_list = []

    logger.info("Started: ELT Job ")

    # Executing ELT pipeline
    for item in dict_param:
        landing_data = extract_data(spark, dict_param[item]["schema"], logger)
        loaded_data = load_data(landing_data, dict_param[item]["source_event_type"], dict_param[item]["event_specification"], logger)
        df_list.append(loaded_data)

    transform_data(df_list, logger)

    logger.info("Completed: ELT Job ")

    # Stopping SparkContext
    spark.stop()
    logger.info("Stopped SparkContext")

    logger.info("################################################################################")
    logger.info("############# Successfully Finished Execution of Miro_Temp Data Load Job ############")
    logger.info("################################################################################")

