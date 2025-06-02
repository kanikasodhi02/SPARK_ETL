import os
import isodate
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import IntegerType, LongType, StringType
import re
from src.utility import common_utility
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType



# Test cases for get_minutes function
def test_get_minutes():
    assert common_utility.get_minutes("PT1H") == 60  
    assert common_utility.get_minutes("PT30M") == 30  
    assert common_utility.get_minutes("PT1H30M") == 90  
    assert common_utility.get_minutes("PT3H45M") == 225  

# Test cases for is_valid_iso8601_duration function
def test_is_valid_iso8601_duration():
    # Valid durations
    assert common_utility.is_valid_iso8601_duration("P1DT1H") == True
    assert common_utility.is_valid_iso8601_duration("PT1H30M") == True
    assert common_utility.is_valid_iso8601_duration("P1DT2H30M") == True
    assert common_utility.is_valid_iso8601_duration("PT") == True

    # Invalid durations
    assert common_utility.is_valid_iso8601_duration("P1D1T") == False  
    assert common_utility.is_valid_iso8601_duration("P1H") == False  
    assert common_utility.is_valid_iso8601_duration("PT1M60S") == False  

    # Test with empty input
    assert common_utility.is_valid_iso8601_duration("") == False

    # Test with a string that doesn't match the pattern
    assert common_utility.is_valid_iso8601_duration("invalid_format") == False

def test_cast_columns(spark_session):
        data = [
            (1, 'a', 1.1, True),
            (2, 'b', 2.2, False),
            (3, 'c', 3.3, True),
            (4, 'd', 4.4, True)
        ]

        schema = StructType([
            StructField("col1", IntegerType(), True),
            StructField("col2", StringType(), True),
            StructField("col3", FloatType(), True),
            StructField("col4", BooleanType(), True)
        ])

        # Create a data frame
        df = spark_session.createDataFrame(data, schema=schema)

        # Define the dictionary of expected data types
        merged_data = [
            ('col1', 'int', 'recipe'),
            ('col2', 'str', 'recipe'),
            ('col3', 'float', 'recipe'),
            ('col4', 'bool', 'recipe')
        ]

        merged_schema = StructType([
            StructField("fields", StringType()),
            StructField("dtype", StringType()),
            StructField("tablename", StringType())
        ])

        # Create a data frame
        merged_df = spark_session.createDataFrame(merged_data, schema=merged_schema)
        # parsing data according to the defined fields
        parse_df = common_utility.cast_columns(df, merged_df, 'recipe')
        assert df is not None
        assert merged_df is not None
        assert parse_df is not None
        # check if all expected columns are present in the output
        assert df.columns == parse_df.columns
        # check if all rows are present in the output
        assert df.count() == parse_df.count()