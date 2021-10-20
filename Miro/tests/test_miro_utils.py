import sys
import pytest

from Miro_Task.Miro.project_utils.utils import MiroUtils
from pyspark.sql.types import DateType, IntegerType, StructField, StructType, StringType, TimestampType


def setup_module(module):
     print("-----------------Calling Set Up Module------------------")

@pytest.mark.skipif(sys.version_info < (3, 6),reason="Python version too obsolete")
def test_read_schema():

    schema_input_1 = MiroUtils.read_schema("initiator_id StringType(),event StringType(),timestamp StringType(),channel StringType(),device_type StringType(),campaign StringType(),browser_version StringType()")
    schema_input_2 = MiroUtils.read_schema("initiator_id StringType(),event StringType(),timestamp TimestampType(),channel StringType(),device_type StringType(),campaign StringType(),browser_version StringType()")

    schema_output_1 = StructType([
                    StructField("initiator_id", StringType(), True),
                    StructField("event", StringType(), True),
                    StructField("timestamp", StringType(), True),
                    StructField("channel", StringType(), True),
                    StructField("device_type", StringType(), True),
                    StructField("campaign", StringType(), True),
                    StructField("browser_version", StringType(), True)
                ])
    schema_output_2 = StructType([
                    StructField("initiator_id", StringType(), True),
                    StructField("event", StringType(), True),
                    StructField("timestamp", TimestampType(), True),
                    StructField("channel", StringType(), True),
                    StructField("device_type", StringType(), True),
                    StructField("campaign", StringType(), True),
                    StructField("browser_version", StringType(), True)
                ])

    assert schema_input_1 == schema_output_1
    assert type(schema_input_1) == type(schema_output_1)

    assert schema_input_2 == schema_output_2
    assert type(schema_input_2) == type(schema_output_2)

    assert schema_input_1 != schema_output_2
    assert type(schema_input_1) == type(schema_output_2)


def teardown_module(module):
     print("-----------------Calling Tear DownModule------------------")