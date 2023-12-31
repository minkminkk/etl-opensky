import os
import logging
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, to_date

from datetime import datetime, timedelta
import requests
import json
from time import sleep

# Globals
API_LIMIT_PERIOD = timedelta(days = 7)
AIRPORT_ICAO = "EDDF"
DATE_FORMAT = "%Y-%m-%d"
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
    start: datetime,
    end: datetime
):
    """Extract data from OpenSky API, rename columns, and write data into HDFS.
    Partition by departureDate.

    Required command-line args:
        airport [str]: ICAO24 address of airport (case-insensitive)
        start [date - YYYY-MM-DD]: Start date
        end [date - YYYY-MM-DD]: End date
    """
    out_path_hdfs = "hdfs://namenode:8020/flights"
    
    spark = SparkSession.builder \
        .appName("Data extraction") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    for response in send_requests("departure", airport, start, end):
        list_flights = process_response(response)
        df = spark.createDataFrame(list_flights, schema = FLIGHTS_SCHEMA)
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
            .parquet(out_path_hdfs, mode = "overwrite")
            

def send_requests(
    type: str,
    airport_icao: str,
    start: datetime,
    end: datetime,
    period: timedelta = timedelta(days = 1)
):
    """Send requests to OpenSky API and check for 4xx, 5xx response status codes

    Args:
        type [str]: Literal values 'departure' or 'arrival', indicating type of
            flight to/from an airport.
        airport_icao [str]: ICAO code of airport (4 letters, case insensitive).
        start [datetime]: Start datetime.
        end [datetime]: End datetime.
        period [timedelta]: Time period for data in each API call.
            Defaults to 1 day.

    Yields:
        response [requests.Response]: Response from server after checked for
            4xx, 5xx status codes

    Raises:
        ValueError: at invalid argument inputs.
        ConnectionError: at 5xx response status code even after 3 retries.
    """
    # Input validation
    if type not in ("arrival", "departure"):
        raise ValueError("\"type\" must be \"arrival\" or \"departure\".")

    # Warning if requested date range is larger than API limit
    if end - start > API_LIMIT_PERIOD and period > API_LIMIT_PERIOD:
        period = API_LIMIT_PERIOD

        logging.warning(
            "Requested date range larger than API limit " \
            + f"({API_LIMIT_PERIOD.days} days). " \
            + "Splitting into multiple API calls."
        )

    # Extract data from API
    cur_start = start
    while cur_start < end:
        # Convert datetime objects to Epoch timestamp
        if end - cur_start <= period:
            cur_end = end
        else:
            cur_end = cur_start + period
        start_ts = int(cur_start.timestamp())
        end_ts = int(cur_end.timestamp())

        # Fill API URL params
        url = f"https://opensky-network.org/api/flights/{type}" \
            + f"?airport={airport_icao}" \
            + f"&begin={start_ts}" \
            + f"&end={end_ts}"

        # Send request
        logging.info(
            f"Extracting {type} data from " \
            + f"{cur_start.strftime(DATE_FORMAT)} to "\
            + f"{cur_end.strftime(DATE_FORMAT)}"
        )
        response = requests.get(url)

        # Status code 400 means invalid input
        if response.status_code == 400:
            logging.error(f"Invalid input. Ending program...")
            raise ValueError("Invalid input.")

        # If server returns error 404, it is most likely that the data source
        # is not updated and it is most likely that we will have the same 
        # situation if going forward. Therefore stop extraction and proceed to 
        # subsequent processing steps. Users will decide if it will run the 
        # pipeline for the future or not.
        # It is also to prevent users from mistyping years into the future.
        if response.status_code == 404:
            logging.warning(
                f"Server has no data about {type} flights "
                + f"from {cur_start.strftime(DATE_FORMAT)} "
                + f"to {cur_end.strftime(DATE_FORMAT)}."
            )
            logging.warning("Stop data extraction. Proceed to next step.")
            break

        # API sometimes returns 5xx status code, can get data if retry 1-2 times
        # Raise error if it still occurs after 3 retries
        retry_cnt = 0
        if response.status_code == 500:
            while retry_cnt < 3:
                logging.warning(
                    "Server error. Retrying in 5 seconds " \
                        + f"({3 - retry_cnt} times left)"
                )
                retry_cnt += 1
                sleep(5)    # Go easy on server
                print("Sending request again")
                response = requests.get(url)
            raise ConnectionError("Cannot get response from server. \
Try again later")

        yield response

        sleep(1)    # Go easy on server
        cur_start += period


def process_response(response: requests.Response):
    """Process response after checked for 4xx, 5xx status codes

    Arg:
        response [requests.Response]: Response from server.

    Returns:
        list_flights [List[dict]]: Data about extracted flight. 
            Generate 1 list per successful API call.

    Raises:
        Exception: When the response does not contain the expected JSON schema.
    """

    list_flights = json.loads(response.content) # Into list of dicts

    try:
        list_flights[0]["icao24"]
    except KeyError or TypeError:
        logging.error("Unexcepted response schema.")
        raise Exception("Unexpected response schema")

    return list_flights
        

if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("airport",
        help = "airport to be investigated, " \
            + "input as ICAO24 address (4 case-insensitive hexadecimal letters)"
    )
    parser.add_argument("start",
        help = "start date (in YYYY-MM-DD)"
    )
    parser.add_argument("end",
        help = "end date (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    if len(args.airport) != 4:
        raise ValueError("\"airport\" must be 4 letters.")
    
    try:
        args.start = datetime.strptime(args.start, DATE_FORMAT)
        args.end = datetime.strptime(args.end, DATE_FORMAT)
    except ValueError:
        raise ValueError("\"start\", \"end\" dates must be in \
YYYY-MM-DD format.")

    if args.start > args.end:
        raise ValueError("\"start\" must be sooner than \"end\"")

    # Call main function
    main(
        airport = args.airport,
        start = args.start,
        end = args.end
    )