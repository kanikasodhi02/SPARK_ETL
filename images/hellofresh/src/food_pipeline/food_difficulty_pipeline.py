import logging
from pyspark.sql import SparkSession
from food_pipeline.food_difficulty_extract import Extract
from food_pipeline.food_difficulty_transform import  Transform
from food_pipeline.food_difficulty_persist import Persist
from etl_abstract import ETLAbstract
from food_pipeline.recipe_difficulty_insights import recipe_difficulties_wise_insights

from parameter_file import filter_ingredient, persist_path, template_path, recipes_raw_data, recipe_temp_file_name, \
    recipes_folder, ingest_write_path, tablename, ingest_read_path,template_path,recipe_schema,persist_path,dq_writing_path

logger = logging.getLogger("FoodDifficultyPipeline")

class FoodDifficultyPipeline(ETLAbstract):

    def __init__(self):
        self.name = "FoodDifficulty"
        super().__init__(self.name)

    def extract(self):
        extract = Extract(self.spark)
        return extract.extract_data(recipes_folder, recipes_raw_data, template_path,
                                                      recipe_temp_file_name)

    def transform(self, data):
        transform_process = Transform(self.spark)
        transformed_df = transform_process.parse_columns(tablename, data,template_path,recipe_schema)
        logger.info('parsing done')
        transformed_with_dq = transform_process.data_quality_checks(transformed_df,dq_writing_path)
        logger.info('data quality checks done')
        return transformed_with_dq
        

    def load(self, data):
                  
        # initialization of persist class
        persist_process = Persist(self.spark)
        # load data in loading folder folder
        persist_process.load_data(data, ingest_write_path)
        logger.info('Loading of data done')
        
        recipe_difficulties_wise_insights(self.spark, ingest_read_path, filter_ingredient,persist_path)
        logger.info('recipe difficulties insights done')
