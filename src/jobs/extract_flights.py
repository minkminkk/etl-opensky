from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.errors.exceptions.captured import AnalysisException

from typing import Any, Callable
from datetime import datetime
import logging
import requests

from config_services import get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()


def main(airport_icao: str, data_start_ts: int, data_end_ts: int) -> None:

    """Ingest extracted data from OpenSky API to data lake"""
    # Get HDFS URI of current partition
    data_path = "/data_lake/flights"
    data_date = datetime.fromtimestamp(data_start_ts)
    print(f"Extracting flights data in {data_date.strftime('%Y-%m-%d')}...")

    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

    # Extract data into DataFrame
    df_extract = spark.createDataFrame([], schema = SCHEMAS.src_flights)
    for type in ["departure", "arrival"]:
        response = request_opensky(type, airport_icao, data_start_ts, data_end_ts)
        list_flights = process_response(
            response, 
            response_check = lambda res: res.json()[0]["icao24"],
            response_filter = lambda res: res.json()
        )

        df = spark.createDataFrame(
            list_flights, 
            schema = SCHEMAS.src_flights
        )

        # Partition columns flight_year/month/day will be extracted 
        # from firstSeen/lastSeen. Therefore print msg if it has NULLs.
        flight_date_identifier = df["firstSeen"] \
            if type == "departure" else df["lastSeen"]
        if df.filter(flight_date_identifier.isNull()).count() > 0:
            print(f"NULLs detected in {type} flights data \
                (which was intended to be the partition column)")
        
        # Create partition columns
        df = df \
            .withColumn(
                "flight_ts",
                F.timestamp_seconds(flight_date_identifier)
            )
        df = df.withColumns(
            {
                "flight_year": F.year(df["flight_ts"]),
                "flight_month": F.month(df["flight_ts"]),
                "flight_day": F.day(df["flight_ts"])
            }
        ).drop("flight_ts")

        # Merge to common output DataFrame
        df_extract = df_extract.unionByName(df)
    
    # Read current data in current partition to another DataFrame
    jsc = spark._jsc
    fs = spark._jvm.org.apache.hadoop.fs
    fs_conf = fs.FileSystem.get(jsc.hadoopConfiguration())
    
    if fs_conf.exists(fs.Path(data_path)):
        df_cur_partition = spark.read.parquet(data_path) \
            .filter(
                (F.col("flight_year") == data_date.year)
                & (F.col("flight_month") == data_date.month)
                & (F.col("flight_day") == data_date.day)
            )
    
        # If no new data to append then end task
        if df_cur_partition.count() == df_extract.count():
            print(f"No new flights data for {data_date}. Ending...")
            return

        # Extract new data compared to current data
        df_append = df_extract.subtract(df_cur_partition)
    else:
        # If file is written for the first time (no partition exists) 
        # then write all extracted data
        print(f"{data_path} does not exist. Creating directories...")
        df_append = df_extract

    # Write into HDFS
    df_append.limit(10).show()     # for logging added data
    df_append.write \
        .mode("append") \
        .partitionBy("flight_year", "flight_month", "flight_day") \
        .parquet(data_path)
    print(f"Written {df_append.count()} records into {data_path}")


def request_opensky(type: str, airport_icao: str, start_ts: int, end_ts: int) \
    -> requests.Response:
    """Request flights data from OpenSky API"""
    # Input validation
    if type not in ("arrival", "departure"):
        logging.error("\"type\" must be \"arrival\" or \"departure\".")
        raise ValueError("Invalid flight type.")

    # Extract data from API
    url = f"https://opensky-network.org/api/flights/{type}"
    response = requests.get(
        url, params = {"airport": airport_icao, "begin": start_ts, "end": end_ts}
    )

    return response


def process_response(
    response: requests.Response, 
    response_check: Callable = None,
    response_filter: Callable = None
) -> Any:
    """Check and filter response"""
    # Check for 400, 500 status codes
    response.raise_for_status()

    # Check response by function
    if response_check:
        try:
            response_check(response)
        except Exception as e:
            logging.error(e)
            raise Exception("Response check failed.")
    
    # Filter response
    if response_filter:
        try:
            response = response_filter(response)
        except Exception as e:
            logging.error(e)
            raise Exception("Response filter function failed")

    return response


if __name__ == "__main__":
    import argparse
    import os
    
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "airport_icao",
        type = str,
        help = "ICAO code of airport to be investigated"
    )
    parser.add_argument(
        "data_start_ts", 
        type = int, 
        help = "Data start timestamp"
    )
    parser.add_argument(
        "data_end_ts", 
        type = int, 
        help = "Data end timestamp"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Call main function
    main(
        airport_icao = args.airport_icao,
        data_start_ts = args.data_start_ts,
        data_end_ts = args.data_end_ts
    )
