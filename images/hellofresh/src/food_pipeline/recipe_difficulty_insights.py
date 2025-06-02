import logging
import os
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from src.utility import spark_utility

logger = logging.getLogger("FoodDifficultiesInsights")

def recipe_difficulties_wise_insights(spark, ingest_read_path, filter_ingredient,persist_path):

        logger.info("recipe_difficulties_wise_insights started")
        ingested_data = spark_utility.reading_parquet_file(spark, ingest_read_path)
      
        # checking if dataframe is empty or not
        if ingested_data.count() == 0:
            raise Exception("Dataframe is empty")
        if filter_ingredient is None or filter_ingredient == '':
            raise Exception("filter ingredient is empty")

        # checking filter record
        ingredient = ingested_data.where(f.lower("ingredients")
                                               .like(f"%{filter_ingredient}%"))

        logger.debug("filtered ingredient")

        # adding total cooking time column
        ingredient = ingredient.withColumn("total_cook_time", ingredient.prepTime_mins
                                           + ingredient.cookTime_mins)
        logger.debug("total cooking time column added")

        # adding difficulty level column according to cooking time
        ingredient = ingredient.withColumn("difficulty",
                                           f.when(ingredient.total_cook_time > 60, "HARD")
                                           .when(ingredient.total_cook_time < 30, "EASY")
                                           .when((f.col("total_cook_time") >= 30)
                                                 & (f.col("total_cook_time") <= 60), "MEDIUM"))
        logger.debug("added difficulty level column according to cooking time")
        # aggregating according to difficulty level of cooking
        ingredient = ingredient.groupBy("difficulty").agg(
            round(avg("total_cook_time"), 2).alias("avg_total_cooking_time"))
            
        spark_utility.write_csv_file(ingredient, persist_path,header=True)
        logger.debug("difficulties insights added with average time")


