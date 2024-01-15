import logging
from typing import Any, Callable
import argparse
from datetime import date

from configs import SparkConfig, HDFSConfig

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_unixtime, to_timestamp, year, month, day
from pyspark.errors.exceptions.captured import AnalysisException

import requests


def main(airport_icao: str, start_ts: int, end_ts: int) -> None:
    """Ingest extracted data from OpenSky API to data lake"""
    # Get configs
    spark_conf = SparkConfig()
    hdfs_conf = HDFSConfig()

    # Create SparkSession
    spark = SparkSession.builder \
        .master(spark_conf.uri) \
        .getOrCreate()

    # Get HDFS URI of current partition
    execution_date = date.fromtimestamp(start_ts)
    data_path = "/data_lake/flights"
    data_uri = hdfs_conf.uri + data_path
    partition_uri = data_uri \
        + f"/year={execution_date.year}" \
        + f"/month={execution_date.month}" \
        + f"/day={execution_date.day}"

    # Read current data in partition to a DataFrame
    try:
        df_cur_partition = spark.read.parquet(partition_uri) \
            .drop("year", "month", "day")
    except AnalysisException:   # In case partition empty
        df_cur_partition = spark.createDataFrame(
            [], 
            schema = spark_conf.schemas.src_flights
        )

    # Extract data into another DataFrame
    df_extract = spark.createDataFrame(
        [], 
        schema = spark_conf.schemas.src_flights
    )
    for type in ["departure", "arrival"]:
        response = request_opensky(type, airport_icao, start_ts, end_ts)
        list_flights = process_response(
            response, 
            response_check = lambda res: res.json()[0]["icao24"],
            response_filter = lambda res: res.json()
        )

        df = spark.createDataFrame(
            list_flights, 
            schema = spark_conf.schemas.src_flights
        )
        df_extract = df_extract.unionByName(df)
    
    # Compare current data with generated date data
    df_append = df_extract.subtract(df_cur_partition)

    # If no data to append then skip to end of task 
    if df_append.isEmpty():
        logging.info(f"No new flights data for {execution_date}. Ending...")
        return
    
    # Create partition columns
    df_append = df_append.withColumn(
        "departure_ts", 
        to_timestamp(from_unixtime(df_append["firstSeen"]))
    )
    # Split into another line to avoid conflict on firstSeen
    df_append = df_append \
        .withColumn("year", year(df_append["departure_ts"])) \
        .withColumn("month", month(df_append["departure_ts"])) \
        .withColumn("day", day(df_append["departure_ts"])) \
        .drop("departure_ts")
    
    # Write into HDFS
    df_append.show()     # for logging added data
    df_append.write \
        .partitionBy("year", "month", "day") \
        .parquet(data_uri, mode = "append")


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
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("airport_icao",
        help = "ICAO code of airport to be investigated"
    )
    parser.add_argument("start_ts", type = int, help = "Data start timestamp")
    parser.add_argument("end_ts", type = int, help = "Data end timestamp")
    args = parser.parse_args()

    # Call main function
    main(
        airport_icao = args.airport_icao,
        start_ts = args.start_ts,
        end_ts = args.end_ts
    )