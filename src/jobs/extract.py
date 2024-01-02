import os
import logging
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_unixtime, to_timestamp, year, month, day

import requests
import json

# Globals
AIRPORT_ICAO = "EDDF"
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


def main(
    airport: str,
    execution_date: datetime
):
    """Extract data from OpenSky API, rename columns, and write data into HDFS.
    Partition by departureDate.

    Required command-line args:
        airport [str]: ICAO24 address of airport (case-insensitive).
        execution_date [date - YYYY-MM-DD]: Date of data.
    """
    out_path_hdfs = "hdfs://namenode:8020/flights"
    
    spark = SparkSession.builder \
        .appName("Data extraction from OpenSkyAPI") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Extract data
    df_to_import = spark.createDataFrame([], schema = SRC_FLIGHTS_SCHEMA)
    for type in ["departure", "arrival"]:
        response = request_opensky(type, airport, execution_date)
        list_flights = process_response(response)

        df = spark.createDataFrame(list_flights, schema = SRC_FLIGHTS_SCHEMA)
        df_to_import = df_to_import.unionByName(df)
    
    # Calculate partitions
    df_to_import = df_to_import \
        .withColumn(
            "departure_ts", 
            to_timestamp(from_unixtime(df_to_import["firstSeen"]))
        )
    # Split into another line to avoid conflict on firstSeen
    df_to_import = df_to_import \
        .withColumn("year", year(df_to_import["departure_ts"])) \
        .withColumn("month", month(df_to_import["departure_ts"])) \
        .withColumn("day", day(df_to_import["departure_ts"])) \
        .drop("departure_ts")
    
    # Write into HDFS
    df_to_import.show()     # for log checking purposes
    df_to_import.write \
        .partitionBy("year", "month", "day") \
        .parquet(out_path_hdfs, mode = "append")


def request_opensky(
    type: str,
    airport_icao: str,
    execution_date: datetime
):
    """Send requests to OpenSky API and return response.

    Args:
        type [str]: Literal values 'departure' or 'arrival', indicating type of
            flight to/from an airport.
        airport_icao [str]: ICAO code of airport (4 letters, case insensitive).
        execution_date [datetime]: Date of data.

    Returns:
        response [requests.Response]: Response from server 
            (checked for 4xx, 5xx code).

    Raises:
        HTTPError: Response contains 4xx, 5xx status code.
    """
    # Input validation
    if type not in ("arrival", "departure"):
        raise ValueError("\"type\" must be \"arrival\" or \"departure\".")

    # Warning if requested date range is larger than API limit
    if execution_date > datetime.today():
        raise ValueError("Cannot get data from the future")

    # Extract data from API
    start_ts = int(execution_date.timestamp())
    end_ts = int((execution_date + timedelta(days = 1)).timestamp())

    url = f"https://opensky-network.org/api/flights/{type}" \
        + f"?airport={airport_icao}" \
        + f"&begin={start_ts}" \
        + f"&end={end_ts}"

    logging.info(
        f"Extracting {type} data in " \
        + f"{execution_date.strftime(DATE_FORMAT)}"
    )
    response = requests.get(url)

    # Check for 4xx, 5xx status code
    response.raise_for_status()

    return response
    

def process_response(response: requests.Response):
    """Process response (evaluate status code, parse response)

    Arg:
        response [requests.Response]: Response from server.

    Returns:
        list_flights [List[dict]]: Data about extracted flight. 
            Generate 1 list per successful API call.

    Raises:
        Exception: When the response does not contain the expected JSON schema.
    """
    logging.info("Response:", response.content)
    list_flights = json.loads(response.content) # Into list of dicts

    for name in SRC_FLIGHTS_SCHEMA.fieldNames():
        try:
            list_flights[0][name]
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
    parser.add_argument("execution_date",
        help = "date of data (in YYYY-MM-DD)"
    )
    args = parser.parse_args()

    # Specify timezone as UTC so that strptime can parse date strings into UTC
    # instead of local time (No effect to actual environment variables)
    os.environ["TZ"] = "Europe/London"

    # Preliminary input validation
    if len(args.airport) != 4:
        raise ValueError("\"airport\" must be 4 letters.")
    
    try:
        args.execution_date = datetime.strptime(args.execution_date, DATE_FORMAT)
    except ValueError:
        raise ValueError("\"execution_date\" must be in YYYY-MM-DD format.")

    # Call main function
    main(
        airport = args.airport,
        execution_date = args.execution_date
    )