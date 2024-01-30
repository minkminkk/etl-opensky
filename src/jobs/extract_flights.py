from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.errors.exceptions.captured import AnalysisException

from typing import Any, Callable
from datetime import datetime
import logging
import requests

from configs import get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()


def main(airport_icao: str, start_ts: int, end_ts: int) -> None:
    """Ingest extracted data from OpenSky API to data lake"""
    # Get HDFS URI of current partition
    execution_date = datetime.fromtimestamp(start_ts)
    data_path = "/data_lake/flights"
    partition_path = data_path \
        + f"/depart_year={execution_date.year}" \
        + f"/depart_month={execution_date.month}" \
        + f"/depart_day={execution_date.day}"

    # Create SparkSession
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .getOrCreate()

    # Extract data into DataFrame
    df_extract = spark.createDataFrame([], schema = SCHEMAS.src_flights)
    for type in ["departure", "arrival"]:
        response = request_opensky(type, airport_icao, start_ts, end_ts)
        list_flights = process_response(
            response, 
            response_check = lambda res: res.json()[0]["icao24"],
            response_filter = lambda res: res.json()
        )

        df = spark.createDataFrame(
            list_flights, 
            schema = SCHEMAS.src_flights
        )
        df_extract = df_extract.unionByName(df)

    # Read current data in partition to another DataFrame
    try:
        df_cur_partition = spark.read.parquet(partition_path) \
            .drop("depart_year", "depart_month", "depart_day")
    except AnalysisException:   # In case partition empty
        df_cur_partition = spark.createDataFrame(
            [], 
            schema = SCHEMAS.src_flights
        )
    
    # If no new data to append then end task 
    if df_cur_partition.count() == df_extract.count():
        print(f"No new flights data for {execution_date}. Ending...")
        return "skipped"
    
    # Create partition columns
    df_append = df_extract.subtract(df_cur_partition)
    df_append = df_append.withColumn(
        "departure_ts", 
        F.to_timestamp(F.from_unixtime(df_append["firstSeen"]))
    )
    # Split into another line to avoid conflict on firstSeen
    df_append = df_append.withColumns(
        {
            "depart_year": F.year(df_append["departure_ts"]),
            "depart_month": F.month(df_append["departure_ts"]),
            "depart_day": F.day(df_append["departure_ts"])
        }
    ).drop("departure_ts")
    
    # Write into HDFS
    df_append.limit(10).show()     # for logging added data
    df_append.write \
        .partitionBy("depart_year", "depart_month", "depart_day") \
        .parquet(data_path, mode = "append")


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
    parser.add_argument("airport_icao",
        help = "ICAO code of airport to be investigated"
    )
    parser.add_argument("start_ts", type = int, help = "Data start timestamp")
    parser.add_argument("end_ts", type = int, help = "Data end timestamp")
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Call main function
    main(
        airport_icao = args.airport_icao,
        start_ts = args.start_ts,
        end_ts = args.end_ts
    )