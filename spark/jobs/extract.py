import os
import logging
import argparse

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, to_date

from datetime import datetime, timedelta
import requests
import json
from time import sleep

# Globals
DATETIME_FORMAT = "%Y-%m-%d"
FLIGHTS_SCHEMA = StructType([
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
    StructField("arrivalAirportCandidatesCount", ShortType()),
])


def main(
    airport: str,
    start_dt: datetime,
    end_dt: datetime
):
    spark = SparkSession.builder \
        .appName("Data extraction") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    # sc = spark.sparkContext

    out_path = "hdfs://namenode:8020/flights"

    flight_batches = extract_flights_by_airport(
        "departure", 
        airport,
        start_dt,
        end_dt
    )

    # TODO: Benchmark createDataFrame(batch) vs createDataFrame(parallelize(batch))
    for batch in flight_batches:
        df = spark.createDataFrame(batch, schema = FLIGHTS_SCHEMA)
        df = df \
            .withColumn(
                "firstSeen", 
                to_timestamp(from_unixtime(df["firstSeen"]))
            ) \
            .withColumn(
                "lastSeen", 
                to_timestamp(from_unixtime(df["lastSeen"]))
            )
        df = df.withColumn(
                "departureDate", 
                to_date(df["firstSeen"])
            ) # Split into another line to avoid conflict on firstSeen
            
        df.write \
            .partitionBy("departureDate") \
            .parquet(out_path, mode = "overwrite")
            

def extract_flights_by_airport(
    type: str,
    airport_icao: str,
    start_dt: datetime,
    end_dt: datetime,
    period: timedelta = timedelta(days = 1)
):
    """Extract flights data to/from a particular airport using OpenSky API

    Args:
        type [str]: Literal values 'departure' or 'arrival', indicating type of
            flight to/from an airport.
        airport_icao [str]: ICAO code of airport (4 letters, case insensitive).
        start_dt [datetime]: Start datetime.
        end_dt [datetime]: End datetime.
        period [timedelta]: Time period for data in each API call.

    Yields:
        flight [List[dict]]: Data about extracted flight.

    Raises:
        ValueError: at invalid argument inputs.
    """
    API_LIMIT_PERIOD = timedelta(days = 7)

    # Input validation
    if type not in ("arrival", "departure"):
        raise ValueError("\"type\" must be \"arrival\" or \"departure\".")

    # Warning if requested date range is larger than API limit
    if end_dt - start_dt > API_LIMIT_PERIOD \
        and period > API_LIMIT_PERIOD:
        period = API_LIMIT_PERIOD

        logging.warning(
            "Requested date range larger than API limit " \
            + f"({API_LIMIT_PERIOD.days} days). " \
            + "Splitting into multiple API calls."
        )

    # Extract data from API
    cur_start_dt = start_dt
    while cur_start_dt < end_dt:
        # Convert datetime objects to Epoch timestamp
        if end_dt - cur_start_dt <= period:
            cur_end_dt = end_dt
        else:
            cur_end_dt = cur_start_dt + period
        start_ts = int(cur_start_dt.timestamp())
        end_ts = int(cur_end_dt.timestamp())

        # Fill API URL params
        url = f"https://opensky-network.org/api/flights/{type}" \
            + f"?airport={airport_icao}" \
            + f"&begin={start_ts}" \
            + f"&end={end_ts}"

        # Send request and parse json
        logging.info(
            f"Extracting {type} data from " \
            + f"{cur_start_dt.strftime(DATETIME_FORMAT)} to "\
            + f"{cur_end_dt.strftime(DATETIME_FORMAT)}"
        )
        response = requests.get(url)

        # Usually if status code 400 means server did not have 
        # data for the requested time. Stop extraction
        if response.status_code == 404:
            logging.critical(f"Invalid response. Ending data extraction.")
            break

        # API sometimes returns 5xx status code, can get data if retry 1-2 times
        # Raise error if it still occurs after 3 retries
        retry_cnt = 0
        while response.status_code == 500 and retry_cnt < 3:
            logging.warning(
                f"Server error. Retrying in 3 seconds ({3-retry_cnt} times left)"
            )
            retry_cnt += 1
            sleep(3)
            response = requests.get(url)
        response.raise_for_status()

        list_flights = json.loads(response.content) # Into list of dicts
        yield list_flights
        
        cur_start_dt += period


if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("airport",
        help = "airport to be investigated, " \
            + "input as ICAO24 address (4 case-insensitive hexadecimal letters)"
    )
    parser.add_argument("start_dt",
        help = "start datetime (in YYYY-MM-DD)"
    )
    parser.add_argument("end_dt",
        help = "end datetime (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    if len(args.airport) != 4:
        raise ValueError("\"airport\" must be 4 letters.")
    
    try:
        args.start_dt = datetime.strptime(args.start_dt, DATETIME_FORMAT)
        args.end_dt = datetime.strptime(args.end_dt, DATETIME_FORMAT)
    except ValueError:
        raise ValueError("\"start_dt\", \"end_dt\" dates must be in \
YYYY-MM-DD format.")

    # Call main function
    main(
        airport = args.airport,
        start_dt = args.start_dt,
        end_dt = args.end_dt
    )