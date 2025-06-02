import os
import isodate
import shutil
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import IntegerType, LongType, StringType
import re


def get_minutes(str_time):
    # parsing pt format type time with the help of iso date
    minutes = round((isodate.parse_duration(str_time).total_seconds()) / 60)
    return minutes

def is_valid_iso8601_duration(str_time): 
    pattern = r'^P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:\d+H)?(?:[0-5]?\dM)?(?:[0-5]?\dS)?)?$'
    return bool(re.match(pattern, str_time))

# df is source dataframe and dftype is dataype info dataframe
def cast_columns(df, dftype, table):
    dftype = dftype.where(dftype.tablename == table).collect()
    for row in dftype:
        if row["dtype"] == 'int':
            df = df.withColumn(row["fields"], col(row["fields"]).cast(IntegerType()))
        if 'bigint' in row["dtype"]:
            df = df.withColumn(row["fields"], col(row["fields"]).cast(LongType()))
        if 'varchar' in row["dtype"] or 'longtext' in row["dtype"] or 'nvarchar' in row["dtype"] or \
                'string' in row["dtype"]:
            df = df.withColumn(row["fields"], col(row["fields"]).cast(StringType()))
        if row["dtype"] == 'datetime':
            df = df.withColumn(row["fields"], to_timestamp(row["fields"], "dd-MM-yyyy HH:mm"))
        if row["dtype"] == 'date':
            df = df.withColumn(row["fields"], to_date(row["fields"], "yyyy-MM-dd"))
    return df



