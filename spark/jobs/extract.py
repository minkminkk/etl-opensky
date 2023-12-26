import os
import logging
import argparse

from pyspark.sql import SparkSession
from pyspark import SparkConf
from datetime import datetime, timedelta
import requests
import json
from time import sleep

# Configs
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level = logging.INFO)


def main(
    airport: str,
    start_dt: datetime,
    end_dt: datetime
):
    
    spark = SparkSession.builder \
        .appName("Data extraction") \
        .getOrCreate()
    sc = spark.sparkContext

    out_path = "hdfs://namenode:8020/flights.parquet"

    flight_batches = extract_flights_by_airport(
        "departure", 
        airport,
        start_dt,
        end_dt
    )
    for batch in flight_batches:
        rdd = sc.parallelize(batch)
        df = spark.createDataFrame(rdd)
        df.write.parquet(out_path, mode = "append")

def extract_flights_by_airport(
    type: str,
    airport_icao: str,
    start_dt: datetime,
    end_dt: datetime
):
    """Extract flights data to/from a particular airport using OpenSky API

    Args:
        type [str]: Literal values 'departure' or 'arrival', indicating type of
            flight to/from an airport.
        airport_icao [str]: ICAO code of airport (4 letters, case insensitive).
        start_dt [datetime]: Start datetime.
        end_dt [datetime]: End datetime.

    Yields:
        flight [dict]: Data about extracted flight.
            |-- icao24
            |-- firstSeen
            |-- estDepartureAirport
            |-- lastSeen
            |-- estArrivalAirport
            |-- callsign
            |-- estDepartureAirportHorizDistance
            |-- estDepartureAirportVertDistance
            |-- estArrivalAirportHorizDistance
            |-- estArrivalAirportVertDistance
            |-- departureAirportCandidatesCount
            |-- arrivalAirportCandidatesCount

    Raises:
        ValueError: at invalid argument inputs.
    """
    API_LIMIT_PERIOD_HOURS = 7 * 24 # hours
    limit_period = timedelta(hours = API_LIMIT_PERIOD_HOURS)

    # Input validation
    if type not in ("arrival", "departure"):
        raise ValueError("\"type\" must be \"arrival\" or \"departure\".")

    # Warning if requested date range is larger than API limit
    if end_dt - start_dt > limit_period:
        logging.warning(
            "Requested date range larger than API limit " \
            + f"({API_LIMIT_PERIOD_HOURS} hours). " \
            + "Splitting into multiple API calls."
        )

    # Extract data from API
    cur_start_dt = start_dt
    while cur_start_dt < end_dt:
        # Convert datetime objects to Epoch timestamp
        if end_dt - cur_start_dt <= limit_period:
            cur_end_dt = end_dt
        else:
            cur_end_dt = cur_start_dt + limit_period
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

        if response.text == "[]":
            logging.warning(f"No flights found from {type} API call")

        list_flights = json.loads(response.content) # Into list of dicts
        yield list_flights

        cur_start_dt += limit_period





if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("airport",
        help = "airport to be investigated, " \
            + "input as ICAO24 address (4 case-insensitive hexadecimal letters)"
    )
    parser.add_argument("start_dt",
        help = "start datetime (in YYYY-MM-DD HH:MM:SS)"
    )
    parser.add_argument("end_dt",
        help = "end datetime (in YYYY-MM-DD HH:MM:SS)"
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
YYYY-MM-DD HH:MM:SS format.")

    # Call main function
    main(
        airport = args.airport,
        start_dt = args.start_dt,
        end_dt = args.end_dt
    )