import os
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, year, month, day, \
    weekofyear, dayofweek
from pyspark.errors.exceptions.captured import AnalysisException
import pyspark.pandas as ps

SCHEMA_DIM_DATES = StructType([
    StructField("date_dim_id", IntegerType()),
    StructField("date", DateType()),
    StructField("year", ShortType()),
    StructField("month", ByteType()),
    StructField("day", ByteType()),
    StructField("week", ByteType()),
    StructField("day_of_week", ByteType())
])


def main():
    start_date = datetime(2015, 1, 1)
    end_date = datetime(2025, 1, 1)
    data_path = "hdfs://namenode:8020/data_warehouse/dim_dates"

    spark = SparkSession.builder \
        .appName("Data transformation") \
        .master("spark://spark-master:7077") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read current data in dim_dates table
    try:
        df_cur = spark.read.table("dim_dates")
    except AnalysisException:
        df_cur = spark.createDataFrame([], schema = SCHEMA_DIM_DATES)
    
    df_dates = spark.createDataFrame([], schema = SCHEMA_DIM_DATES)
    df_dates = df_dates \
        .withColumn("date", ps.date_range(start_date, end_date))
    df_dates = df_dates \
        .withColumn("date_dim_id", date_format(df_dates["date"], "yyyyMMdd")) \
        .withColumn("year", year(df_dates["date"])) \
        .withColumn("month", month(df_dates["date"])) \
        .withColumn("day", day(df_dates["date"])) \
        .withColumn("week", weekofyear(df_dates["date"])) \
        .withColumn("day_of_week", dayofweek(df_dates["date"]))
    df_dates.show()

    df_append = df_dates.subtract(df_cur)
    df_append.show()


if __name__ == "__main__":
    main()