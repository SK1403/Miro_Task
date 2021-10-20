import configparser
import os
import sys
import pytest

from Miro_Task.Miro.jobs.miro_elt_jobs import *
from Miro_Task.Miro.project_utils.log4j_root_logger import *
from Miro_Task.Miro.project_utils.utils import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType


class tempSparkSession:
    """
        Creating MOCK class for providing dummy input
    """

    def __init__(self):
        self.df = None
        self.logger = None

    def createSparkSession(self) -> SparkSession:
        return SparkSession.builder \
            .master("local") \
            .appName("Miro TEST Task") \
            .getOrCreate()

    def get_data(self, spark: SparkSession) -> DataFrame:
        path = os.getcwd()
        print(path)
        config = configparser.ConfigParser()
        config.read(r"{}/../config/config.ini".format(PATH))
        schema_config = config.get('schema', 'landingFileSchema')
        schema = MiroUtils.read_schema(schema_config)
        self.df = spark.read.schema(schema).json(os.path.join(path, "*.json"))
        return self.df

    def get_logger(self, spark: SparkSession) -> Log4jRootLogger:
        return Log4jRootLogger(spark)

    def close(self, SparkSession) -> None:
        SparkSession.stop()


'''
    Run tests in local mode by calling py.test -m spark_local or in YARN with py.test -m spark_yarn
'''
@pytest.fixture(scope='module')
def sparkSession():
    print("-----------------Calling Set Up Module------------------")
    sparkSession = tempSparkSession()
    spark = sparkSession.createSparkSession()
    # return sparkSession
    yield spark
    print("-----------------Calling Tear DownModule------------------")
    sparkSession.close(spark)

@pytest.mark.skipif(sys.version_info < (3, 6),reason="Python version too obsolete")
def test_mock_read(sparkSession):
    sparkConnect = tempSparkSession()
    df = sparkConnect.get_data(sparkSession)
    assert df.count() == 10
    assert df.count() != 0

@pytest.mark.skipif(sys.version_info < (3, 6),reason="Python version too obsolete")
def test_mock_schema(sparkSession):
    sparkConnect = tempSparkSession()
    df = sparkConnect.get_data(sparkSession)

    input_schema = df.schema

    expected_schema_1 = StructType([
        StructField("initiator_id", StringType(), True),
        StructField("event", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("browser_version", StringType(), True)
    ])

    expected_schema_2 = StructType([
        StructField("initiator_id", StringType(), True),
        StructField("event", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("channel", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("browser_version", StringType(), True)
    ])

    assert input_schema != expected_schema_1
    assert input_schema == expected_schema_2






