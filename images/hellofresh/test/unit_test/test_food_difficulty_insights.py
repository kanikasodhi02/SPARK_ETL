import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, BooleanType
from test.unit_test.parameter_file_test import recipes_folder, recipes_empty_folder,recipes_folder_test,test_recipes_raw_data, template_path, recipe_temp_file_name
from src.food_pipeline import recipe_difficulty_insights
from src.utility import spark_utility

def test_recipe_difficulties_wise_insights(spark_session):

        test_empty_df = './test/data/test_empty_data/' 
        directory_recipes = './test/data/test_load_data/'
        directory_csv = './test/data/output/'
        
        '''with pytest.raises(Exception, match=r"Dataframe is empty"):
            recipe_difficulty_insights.recipe_difficulties_wise_insights(spark_session,test_empty_df,'onion',directory_csv)
        '''
        # Empty ingredient filter test'
        
        with pytest.raises(Exception, match=r"filter ingredient is empty"):
            recipe_difficulty_insights.recipe_difficulties_wise_insights(spark_session,directory_recipes, '',directory_csv)


        filter_ingredient = 'water'
        
        recipe_difficulty_insights.recipe_difficulties_wise_insights(spark_session,directory_recipes, filter_ingredient,directory_csv)
        
        recipe_difficulties_df = spark_utility.reading_csv_file(spark_session,directory_csv,header=True)
        
        
        expected_df = spark_session.createDataFrame([('EASY', '18.38'),('MEDIUM', '30.0'), ('HARD', '85.0')],
                                                 schema="difficulty STRING, avg_total_cooking_time STRING")
      

        assert recipe_difficulties_df.count() == expected_df.count()
        assert recipe_difficulties_df.collect() == expected_df.collect()