import logging

from pyspark.sql.types import IntegerType,StringType
import pyspark.sql.functions as f
from pyspark.sql.functions import md5, concat_ws
from src.utility import spark_utility,common_utility

logger = logging.getLogger("FoodDifficultyTransform")

class Transform:
    def __init__(self, spark):
        self.spark = spark

    def parse_columns(self, tablename, json_df,template_path,recipe_schema):

        logger.info("parsing of column started")
        # reading data from csv template file
        reading_template = spark_utility.reading_csv_file(self.spark, template_path + recipe_schema,
                                                          header=True)
       
        # checking table name must not be null
        if tablename == '':
            raise Exception("table name is empty")
        # parsing data according to the defined fields
        parse_df = common_utility.cast_columns(json_df, reading_template, tablename)
        logger.info("parsing of the column done")
        return parse_df

    def rempove_duplicates(self,json_data):
        # dropping duplicate rows
        json_data_without_duplicate = json_data.dropDuplicates()
        return json_data_without_duplicate
        
    def is_valid_udf(self,json_data_without_duplicate):
        df_with_replaced_values = json_data_without_duplicate\
            .withColumn("cookTime_replaced",
                         f.when(f.col("cookTime").isNull() | (f.col("cookTime") == ""), "Invalid").otherwise(f.col("cookTime")))\
            .withColumn("prepTime_replaced",
                        f.when(f.col("prepTime").isNull() | (f.col("prepTime") == ""), "Invalid").otherwise(f.col("prepTime")))

        
        # Converting function to UDF
        is_valid_udf = f.udf(lambda z: common_utility.is_valid_iso8601_duration(z), StringType())

        # Use the UDF to check if the str_time is a valid ISO 8601 duration format
        df_with_validation = df_with_replaced_values\
            .withColumn("validation_cookTime_status", is_valid_udf("cookTime_replaced").cast("boolean"))\
            .withColumn("validation_prepTime_status", is_valid_udf("prepTime_replaced").cast("boolean"))
            
        return df_with_validation
    
    def data_quality_checks(self, json_data,writing_path):
        logger.info("Quality data started")
        # dropping duplicate rows
        json_data_without_duplicate = self.rempove_duplicates(json_data)
        logger.info("dropping rows duplicate records values")


        df_with_validation = self.is_valid_udf(json_data_without_duplicate)
        
        writing_path_test = "/data/test/"
        # dq_rejected records
        spark_utility.write_csv_file(df_with_validation,writing_path_test)

        # seggregate the invalid records
        invalid_records = df_with_validation[
            (~df_with_validation['validation_cookTime_status']) &
            (~df_with_validation['validation_prepTime_status'])
            ]
       
        # dq_rejected records
        spark_utility.write_csv_file(invalid_records,writing_path)

        # proceed valid records
        valid_records = df_with_validation[
            (df_with_validation['validation_cookTime_status']) &
            (df_with_validation['validation_prepTime_status'])
            ]
        # Converting function to UDF
        get_minutes_UDF = f.udf(lambda z: common_utility.get_minutes(z), IntegerType())

        format_json_data = valid_records.select(valid_records.ingredients,
                                                         valid_records.datePublished, valid_records.description,
                                                         valid_records.name,
                                                         valid_records.image, valid_records.recipeYield, valid_records.url,
                                                         valid_records.cookTime,valid_records.prepTime,
                                                         get_minutes_UDF(f.col("cookTime_replaced")).alias("cookTime_m"),
                                                         get_minutes_UDF(f.col("prepTime_replaced")).alias("prepTime_m")) \
            .withColumn("activeRecords", f.lit(0))
        logger.info("Quality validation of iso and get minutes completed ")

        # casting of cooktime and preparetime to integer
        format_json_data = format_json_data \
            .withColumn("cookTime_mins", format_json_data.cookTime_m.cast(IntegerType())) \
            .withColumn("prepTime_mins", format_json_data.prepTime_m.cast(IntegerType()))
            
        format_json_data = format_json_data.select(format_json_data.ingredients, format_json_data.datePublished,
                                                   format_json_data.description,
                                                   format_json_data.name, format_json_data.image,
                                                   format_json_data.recipeYield, format_json_data.url,
                                                   format_json_data.cookTime_mins,
                                                   format_json_data.prepTime_mins, format_json_data.activeRecords)

        logger.info("Quality data passed")
        return format_json_data



