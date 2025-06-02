import logging

from src.utility import spark_utility

logger = logging.getLogger("FoodDifficultyPersist")


class Persist:
    def __init__(self, spark):
        self.spark = spark
        
    def load_data(self, format_json_data, write_path):
        partition_columns = ['cookTime_mins','prepTime_mins']

        spark_utility.write_parquet_file(format_json_data, write_path,partition_columns)
        logger.info("data loaded into ingest folder")

   
