import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
from test.unit_test.parameter_file_test import recipes_folder, recipes_empty_folder,recipes_folder_test,test_recipes_raw_data, template_path, recipe_temp_file_name
from src.food_pipeline.food_difficulty_persist import Persist
from src.utility import spark_utility,common_utility


def test_load_data(spark_session):
        input_data = spark_session.createDataFrame(
            [("Salt", "2023-04-28", "This is a recipe for salt.", "Salt Recipe",
              "salt.png", "2 servings", "https://www.saltrecipe.com",
              20, 5, 0),
             ("Water", "2023-04-28", "This is a recipe for water.", "Water Recipe",
              "water.png", "1 serving", "https://www.waterrecipe.com",
              5, 2, 0)], ["ingredients", "datePublished", "description",
                          "name", "image", "recipeYield", "url",
                          "cookTime_mins", "prepTime_mins", "activeRecords"
                          ])

        data = Persist(spark_session)
        path = './test/data/test_load_data/'
        data.load_data(input_data,path)

        # reading parquet file
        load_data = spark_utility.reading_parquet_file(spark_session, path)

        # assert
        assert load_data.collect(), input_data.collect()