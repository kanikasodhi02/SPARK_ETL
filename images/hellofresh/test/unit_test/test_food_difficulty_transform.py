import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
from test.unit_test.parameter_file_test import recipes_folder, recipes_empty_folder,recipes_folder_test,test_recipes_raw_data, template_path, recipe_temp_file_name
from src.food_pipeline.food_difficulty_transform import Transform
from src.utility import spark_utility,common_utility


def test_parse_columns(spark_session):

        data = Transform(spark_session)
        # test recipe folder
        recipes_folder = './test/data/test_data/'
        # reading data from raw file
        test_recipes_raw_data = 'recipes-000.json'
        tablename = 'recipes'
        template_path = './test/data/template/'
        # file name of recipe schema
        recipe_temp_file_name = 'recipeschema.csv'

        # reading json file
        json_data = spark_utility.reading_json_file(spark_session,recipes_folder + test_recipes_raw_data)

        # valid scenario
        result = data.parse_columns(tablename,json_data,template_path,recipe_temp_file_name)
        assert result is not None
        assert result.count() == 484

        # invalid scenario
        tablename = ''
        with pytest.raises(Exception, match=r"table name is empty"):
            data.parse_columns(tablename, json_data,template_path,recipe_temp_file_name)

def test_data_quality_checks(spark_session):
        input_json_data = spark_session.createDataFrame(
            [("Salt", "2023-04-28", "This is a recipe for salt.", "Salt Recipe",
              "salt.png", "2 servings", "https://www.saltrecipe.com",
              "PT20M", "PT5M"),
             ("Water", "2023-04-28", "This is a recipe for water.", "Water Recipe",
              "water.png", "1 serving", "https://www.waterrecipe.com",
              "PT5M", "invalid"),
             ("Salt", "2023-04-28", "This is a recipe for salt.", "Salt Recipe",
              "salt.png", "2 servings", "https://www.saltrecipe.com",
              "PT20M", "PT5M")], ["ingredients", "datePublished", "description",
                                  "name", "image", "recipeYield", "url",
                                  "cookTime", "prepTime"])
        expected_result = spark_session.createDataFrame(
            [("Salt", "2023-04-28", "This is a recipe for salt.", "Salt Recipe",
              "salt.png", "2 servings", "https://www.saltrecipe.com",
              20, 5, 0)], ["ingredients", "datePublished", "description",
                          "name", "image", "recipeYield", "url",
                          "cookTime_mins", "prepTime_mins", "activeRecords"
                          ])

        data = Transform(spark_session)
        path = './test/data/dq_rejected_file/'
        result = data.data_quality_checks(input_json_data,path)
        
        # Assert
        assert result is not None
        assert result.columns == expected_result.columns
        assert result.count() == expected_result.count()
        assert result.collect() == expected_result.collect()





