import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, year, month, day

from configs import HDFSConfig, SparkConfig
from config.general import DATE_FORMAT
from config.schemas import SRC_FLIGHTS_SCHEMA
from config.paths import SPARK_MASTER_URI, HDFS_URI_PREFIX


def main(execution_date: datetime):
    """Transformation on extracted data
    
    
    """
    data_uri = "hdfs://namenode:8020/data_lake/flights"
    partition_uri = data_uri \
        + f"/year={execution_date.year}" \
        + f"/month={execution_date.month}" \
        + f"/day={execution_date.day}"

    spark = SparkSession.builder \
        .appName("Data transformation") \
        .master(SPARK_MASTER_URI) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Read current data
    df_cur_partition = spark.read.parquet(partition_uri)
    
    # Filter & rename columns, parse ts into dates & times
    df_cur_partition = df_cur_partition \
        .select(
            "icao24",
            "firstSeen",
            "estDepartureAirport",
            "lastSeen",
            "estArrivalAirport",
            "callsign"
        ) \
        .withColumnRenamed("icao24", "aircraft_icao24") \
        .withColumnRenamed("firstSeen", "depart_ts") \
        .withColumnRenamed("estDepartureAirport", "depart_airport") \
        .withColumnRenamed("lastSeen", "arrival_ts") \
        .withColumnRenamed("estArrivalAirport", "arrival_airport")
    
    df_cur_partition = df_cur_partition \
        .withColumn(
            "depart_dim_date_id", 
            from_unixtime(df_cur_partition["depart_ts"], "yyyyMMdd")
        ) \
        .withColumn(
            "depart_ts", 
            to_timestamp((df_cur_partition["depart_ts"]))
        ) \
        .withColumn(
            "arrival_dim_date_id", 
            from_unixtime(df_cur_partition["arrival_ts"], "yyyyMMdd")
        ) \
        .withColumn(
            "arrival_ts", 
            to_timestamp((df_cur_partition["arrival_ts"]))
        )
    df_cur_partition.show()


if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("execution_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    try:
        args.execution_date = datetime.strptime(args.execution_date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(execution_date = args.execution_date)