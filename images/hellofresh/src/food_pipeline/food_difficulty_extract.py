import os
import glob
from src.utility import spark_utility
import logging

logger = logging.getLogger("FoodDifficultyExtract")

class Extract:
    def __init__(self, spark):
        self.spark = spark


    def check_directory_and_file(self,directory_path, file_pattern):
     # Check if the directory exists
     if os.path.exists(directory_path):
        # Check if the directory is a directory (not a file or a symlink)
        if os.path.isdir(directory_path):
            # Use glob to find files matching the pattern in the directory
            files_matching_pattern = glob.glob(os.path.join(directory_path, file_pattern))
            if not files_matching_pattern:
                raise Exception(f"No files matching the pattern in the directory. {files_matching_pattern}")
        else:
            raise Exception(f"The provided path is not a directory. {files_matching_pattern}")
     else:
        raise Exception(f"Directory does not exist. {directory_path}")


    def validate_schema_match(self,json_col_type, csv_col_type):
        if not json_col_type or not csv_col_type:
            raise ValueError(f"List is empty")

        if json_col_type != csv_col_type:
            raise ValueError("Template and JSON list fields are not equal")


    def extract_data(self, recipes_folder, raw_data, template_path, recipe_temp_file_name):
        logger.info("Schema validation started")

        self.check_directory_and_file(recipes_folder,raw_data)

        files_path = recipes_folder + raw_data
        # reading raw json file
        json_data = spark_utility.reading_json_file(self.spark, files_path)
        if json_data.count() == 0:
            raise Exception(f"File is empty. Path = {files_path}")

        # reading tempalate
        template_data = spark_utility.reading_csv_file(self.spark, template_path + recipe_temp_file_name,header=True)

        # validating columns
        self.validate_schema_match(json_data.columns, template_data.columns)
        logger.info("Template and JSON schema match")
        return json_data

