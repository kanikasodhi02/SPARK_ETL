import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
from test.unit_test.parameter_file_test import recipes_folder, recipes_empty_folder,recipes_folder_test,test_recipes_raw_data, template_path, recipe_temp_file_name
from src.food_pipeline.food_difficulty_extract import Extract

def test_extract_data(spark_session):
    data = Extract(spark_session)
    result = data.extract_data(recipes_folder,
                                         test_recipes_raw_data,
                                         template_path,
                                         recipe_temp_file_name)
    assert result.count() == 484


def test_check_directory_and_file_valid(spark_session):
        # Arrange
        extract_obj = Extract(spark_session)
        directory_recipes = './test/data/test_data/'
        test_recipes_raw_data = '*.json'
        # call function check_directory_and_file
        extract_obj.check_directory_and_file(directory_recipes, test_recipes_raw_data)


def test_check_directory_and_file_directory_not_exists(spark_session):
        # Arrange
        extract_obj = Extract(spark_session)
        recipes_folder_test = './rest/data/testing_data/'
        file_name = "*.json"
        # Test with a directory that does not exist
        with pytest.raises(Exception, match="Directory does not exist"):
            extract_obj.check_directory_and_file(recipes_folder_test, file_name)


def test_check_directory_and_file_file_not_found(spark_session):
        # Arrange
        extract_obj = Extract(spark_session)
        directory_recipes = './test/data/test_data/'
        test_recipes_raw_data = '*.txt'
       #   Test with a valid directory but no files matching the pattern
        with pytest.raises(Exception, match="No files matching the pattern in the directory"):
             extract_obj.check_directory_and_file(directory_recipes, test_recipes_raw_data)

def test_validate_schema_match_list_empty(spark_session):
        # Arrange
        extract_obj = Extract(spark_session)
        json_col_type = ['cookTime', 'datePublished', 'description']
        csv_col_type = ['cookTime', 'datePublished', 'description']
        # call function extract_data
        try:
            extract_obj.validate_schema_match(json_col_type, csv_col_type)
        except ValueError as e:
            pytest.fail(f"Unexpected ValueError: {e}")

def test_validate_schema_match_template_not_match(spark_session):
    # Arrange
        extract_obj = Extract(spark_session)
        json_col_type = ['cookTime', 'datePublished']
        csv_col_type = ['cookTime', 'datePublished', 'description']

        #  & Assert
        with pytest.raises(ValueError, match="Template and JSON list fields are not equal"):
            extract_obj.validate_schema_match(json_col_type, csv_col_type)

def test_extract_data_valid(spark_session):
    extract_obj = Extract(spark_session)
    directory_recipes = './test/data/test_data/'
    test_recipes_raw_data = 'recipes-000.json'
    template_path = './test/data/template/'
    # file name of recipe schema
    recipe_temp_file_name = 'recipe_table_template.csv'
    # call function extract_data
    result = extract_obj.extract_data(directory_recipes, test_recipes_raw_data, template_path, recipe_temp_file_name)
    # Assert
    assert result.count() > 0

def test_extract_data_empty_json(spark_session):
    extract_obj = Extract(spark_session)
    directory_recipes = './test/data/test_data_test/'
    test_recipes_raw_data = 'recipes-000.json'
    template_path = './test/data/template/'
    # file name of recipe schema
    recipe_temp_file_name = 'recipe_table_template.csv'
    # Act & Assert
    with pytest.raises(Exception, match="File is empty"):
        extract_obj.extract_data(directory_recipes, test_recipes_raw_data, template_path, recipe_temp_file_name)

