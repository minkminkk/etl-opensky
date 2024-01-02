import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, year, month, day

DATE_FORMAT = "%Y-%m-%d"
SRC_FLIGHTS_SCHEMA = StructType([
    StructField("icao24", StringType(), nullable = False),
    StructField("firstSeen", LongType()),
    StructField("estDepartureAirport", StringType()),
    StructField("lastSeen", LongType()),
    StructField("estArrivalAirport", StringType()),
    StructField("callsign", StringType()),
    StructField("estDepartureAirportHorizDistance", IntegerType()),
    StructField("estDepartureAirportVertDistance", IntegerType()),
    StructField("estArrivalAirportHorizDistance", IntegerType()),
    StructField("estArrivalAirportVertDistance", IntegerType()),
    StructField("departureAirportCandidatesCount", ShortType()),
    StructField("arrivalAirportCandidatesCount", ShortType())
])

def main(execution_date: datetime):
    data_path = "hdfs://namenode:8020/flights"
    partition_path = data_path \
        + f"/year={execution_date.year}" \
        + f"/month={execution_date.month}" \
        + f"/day={execution_date.day}"

    spark = SparkSession.builder \
        .appName("Data transformation") \
        .master("spark://spark-master:7077") \
        .enableHiveSupport() \
        .getOrCreate()
    
    df = spark.read.parquet(partition_path)
    df.show()


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
        args.execution_date = datetime.strptime(args.execution_date, DATE_FORMAT)
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(execution_date = args.execution_date)