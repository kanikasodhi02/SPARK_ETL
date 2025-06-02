# custom code which can be used by every class
import logging

logger = logging.getLogger("spark_utility")


def reading_json_file(spark, json_file):
    # reading raw json data
    read_json = spark.read.json(json_file)
    return read_json


def reading_csv_file(spark, file, header):
    # reading raw json data
    read_csv = spark.read.csv(file, header=header)
    return read_csv


def reading_parquet_file(spark, file):
    # read parquet data
    read_parquet = spark.read.parquet(file)
    return read_parquet


def write_parquet_file(df, path, partition_columns):

    # Check if both partition_columns and bucket_cols are None
    if len(partition_columns) == 0:
        logger.info("partition and bucket is none")
        return df.write.format("parquet").mode('overwrite').save(path)
    else:
        return df.write.mode('overwrite').partitionBy(*partition_columns).parquet(path)


def write_csv_file(df, path,header=True):
    # write DataFrame into csv file
        return df.write.option("header", str(header).lower()).format("csv").mode('overwrite').save(path)
