import pytest
from pyspark.sql import SparkSession

# Define the Spark session fixture
@pytest.fixture(scope="session")
def spark_session(request):
    # Create a Spark session
    spark = SparkSession.builder \
            .appName("HelloFreshSparkTest") \
            .getOrCreate()

    # Set up teardown logic for the fixture
    def teardown():
        spark.stop()

    # Register the teardown function with the request context
    request.addfinalizer(teardown)

    return spark
